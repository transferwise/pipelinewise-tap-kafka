import os
import inspect
import importlib
import importlib.machinery
import sys
import subprocess
import struct

from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer, _ContextStringIO, _MAGIC_BYTE
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema, SchemaReference, RegisteredSchema
from confluent_kafka.serialization import SerializationError

from google.protobuf.json_format import MessageToDict
from google.protobuf.json_format import MessageToJson
from google.protobuf import descriptor_pool, message_factory, proto_builder, descriptor_pb2, message
from google.protobuf.message import DecodeError

from typing import List

from tap_kafka.errors import ProtobufCompilerException


# pylint: disable=R0903
class ProtobufDictDeserializer(ProtobufDeserializer):
    """
    Deserializes a Python dict object from protobuf
    """
    def __call__(self, value, ctx):
        msg =  super().__call__(value, ctx)
        return MessageToDict(msg,
                             preserving_proto_field_name=True,
                             including_default_value_fields=True)


def topic_name_to_protoc_output_name(topic: str) -> str:
    """Convert topic name to the file name that protoc is generating"""
    return topic.replace('-', '_').replace('.', '_')


class ProtobufDeserializerWithRegistry(ProtobufDeserializer):
    """
    ProtobufDeserializerWithRegistry is an extension of ProtobufDeserializer, which can handle decoding bytes without
    knowing message type in advance. In case no message type is passed, deserializer will fetch necessary
    information from the Schema registry.

    TODO: support providing message type in advance
    TODO: refactor the code, too many single-action functions

    Args:
        message_type (GeneratedProtocolMessageType): Protobuf Message type.
        conf (dict): Configuration dictionary.
        schema_registry_client (SchemaRegistryClient): Schema Registry client instance.

    ProtobufDeserializerWithRegistry configuration properties:

    +-------------------------------------+----------+------------------------------------------------------+
    | Property Name                       | Type     | Description                                          |
    +-------------------------------------+----------+------------------------------------------------------+
    | ``file.prefix``                     | str      | Prefix to use for the generated files.               |
    +-------------------------------------+----------+------------------------------------------------------+
    """
    __slots__ = ['_schema_id', '_registry', '_use_dynamic_proto', '_file_prefix', '_proto_pool', '_proto_factory'
                 , '_proto_cache', '_skip_known_types']

    def __init__(self, message_type=None, schema_registry_client=None, conf=None):
        self._proto_pool = descriptor_pool.DescriptorPool()
        self._proto_factory = message_factory.MessageFactory(self._proto_pool)

        # even with message_type provided we need to maintain cache, thus we need prefixes
        if conf is None or 'file.prefix' not in conf:
            raise ValueError("ProtobufDeserializerWithRegistry: the 'file.prefix' configuration"
                             "property must be provided")
        self._file_prefix = conf.pop('file.prefix')

        if not message_type:
            if not schema_registry_client:
                raise ValueError("ProtobufDeserializerWithRegistry: schema_registry_client must be provided"
                                 "in case no message_type is provided!")
            self._use_dynamic_proto = True
            self._registry = schema_registry_client
            # TODO: is this the only way? currently otherwise parent init will fail
            message_type = proto_builder.MakeSimpleProtoClass({'base': descriptor_pb2.FieldDescriptorProto.TYPE_STRING})

        self._proto_cache = {}  # {qualifier name: message class}

        super().__init__(message_type, conf)

    def _get_schema_from_registry(self, schema_id: int) -> Schema:
        """
        Gets schema string from the schema registry.

        Args:
            schema_id (int): schema id to lookup

        Returns:
            str: schema representation as string
        """
        schema: Schema = self._registry.get_schema(schema_id)
        return schema

    @staticmethod
    def _generate_dir_for_files(dir_name) -> None:
        """
        Checks whether directory to store generated files exists and if not, creates one.
        """
        if not os.path.exists(os.path.expanduser(dir_name)):
            os.makedirs(dir_name, exist_ok=True)

    @staticmethod
    def _create_proto_file(schema_f_path: str, schema_str: str) -> None:
        """

        :param schema_str:
        :return:
        """
        with open(schema_f_path, 'w+') as schema_file:
            schema_file.write(schema_str)
            schema_file.flush()

    @staticmethod
    def _generate_descriptor_set_file(dir_name: str, descriptor_f_path: str, proto_f_name: str):
        """

        :return:
        """
        command = f"{sys.executable} -m grpc_tools.protoc -I {dir_name} --include_imports " \
                  f"--descriptor_set_out {descriptor_f_path} {proto_f_name}"

        subprocess.run(command.split(), check=True, stdout=subprocess.PIPE, env=os.environ.copy())

    def _resolve_schema_references(self, schema: Schema):
        references: List[SchemaReference] = schema.references
        if not references:
            return

        for reference in references:
            # needed because there's discrepancy in how references are handled for different endpoints
            # TODO: raise issue in (python-)confluent-kafka
            if isinstance(reference, dict):
                reference = SchemaReference(reference['name'], reference['subject'], reference['version'])
            if reference.name.startswith("google/protobuf"):  # we won't be able to deal with known types anyway
                continue
            referenced_schema: RegisteredSchema = self._registry.get_version(reference.subject, reference.version)
            proto_path = os.path.join(self._file_prefix, reference.name)
            self._create_proto_file(proto_path, referenced_schema.schema.schema_str)
            # need to resolve refs/deps for the referenced schemas as well
            self._resolve_schema_references(referenced_schema.schema)

    def _create_file_descriptor_set(self, schema: Schema, schema_id: int) -> descriptor_pb2.FileDescriptorSet:
        """
        Orchestrator method, which generates FileDescriptorSet given the schema. Also resolves
        all of the dependencies and fetches them.

        :param schema: schema to generate the descriptors from.
        :param schema_id: ID of the schema to generate the descriptors from.
        :return:
        """
        self._generate_dir_for_files(self._file_prefix)

        self._resolve_schema_references(schema)  # first references/dependencies, otherwise everything will fail

        current_prefix = f"{self._file_prefix}_{schema_id}"
        proto_f_name = f"{current_prefix}.proto"
        descriptor_f_name = f"{current_prefix}.pb"

        proto_f_path = os.path.join(self._file_prefix, proto_f_name)
        descriptor_f_path = os.path.join(self._file_prefix, descriptor_f_name)

        self._create_proto_file(proto_f_path, schema.schema_str)
        self._generate_descriptor_set_file(self._file_prefix, descriptor_f_path, proto_f_name)

        with open(descriptor_f_path, 'rb') as descriptor_file:
            descriptor_set = descriptor_file.read()

        file_descriptor_set: descriptor_pb2.FileDescriptorSet = descriptor_pb2.FileDescriptorSet()\
            .FromString(descriptor_set)

        return file_descriptor_set

    def _add_file_descriptors_to_pool(self, file_descriptor_set: descriptor_pb2.FileDescriptorSet) -> None:
        """

        :param file_descriptor_set:
        :return:
        """
        for file_descriptor in file_descriptor_set.file:
            # FIXME: this causes problems with dependencies if they change. File names are kept the same and there's no
            #  replacing of files.
            self._proto_pool.Add(file_descriptor)

    def _get_message_type(self, proto_file_name: str, index: int):
        """

        :param proto_file_name:
        :param index:
        :return:
        """
        messages = self._proto_factory.GetMessages([proto_file_name])
        file_to_search = self._proto_pool.FindFileByName(proto_file_name)
        for idx, message_type_name in enumerate(file_to_search.message_types_by_name):
            if idx == index:
                try:
                    return messages[message_type_name]
                except KeyError:
                    # for the packaged ones the name is prepended with pkg name
                    # FIXME: probably needs to be smarter than catching exception
                    return messages[f"{file_to_search.package}.{message_type_name}"]
        raise ValueError("Message type not found by the index")

    def _retrieve_message_class_from_cache(self, schema_id: int, index: int) -> message.Message:
        """

        :param schema_id:
        :param index:
        :return:
        """
        current_prefix = f"{self._file_prefix}_{schema_id}"
        message_class = self._proto_cache.get(f"{current_prefix}_{index}")

        if not message_class:
            schema = self._get_schema_from_registry(schema_id)
            file_descriptor_set = self._create_file_descriptor_set(schema, schema_id)
            self._add_file_descriptors_to_pool(file_descriptor_set)
            message_class = self._get_message_type(f"{current_prefix}.proto", index)
            self._proto_cache[f"{current_prefix}_{index}"] = message_class

        return message_class

    def __call__(self, value, ctx):
        """

        :param value:
        :param ctx:
        :return:
        """
        if value is None:
            return None

        if len(value) < 6:
            raise SerializationError("Message too small. This message was not"
                                     " produced with a Confluent"
                                     " Schema Registry serializer")

        with _ContextStringIO(value) as payload:
            magic, schema_id = struct.unpack('>bI', payload.read(5))
            if magic != _MAGIC_BYTE:
                raise SerializationError("Unknown magic byte. This message was"
                                         " not produced with a Confluent"
                                         " Schema Registry serializer")
            # all indexes are appended to the left
            index = self._decode_index(payload, zigzag=not self._use_deprecated_format)[0]

            message_class = self._retrieve_message_class_from_cache(schema_id, index)()

            try:
                message_class.ParseFromString(payload.read())
            except DecodeError as e:
                raise SerializationError(str(e))

            return message_class


# pylint: disable=R0914
def proto_to_message_type(schema: str, protobuf_classes_dir: str, topic: str):
    """Compile a protobuf schema to python class and load it dynamically"""
    mod_name = f"proto_message_{topic_name_to_protoc_output_name(topic)}"
    proto_name = f"{mod_name}.proto"
    module_name = f"{mod_name}_pb2"
    module_filename = f"{module_name}.py"

    if not os.path.exists(os.path.expanduser(protobuf_classes_dir)):
        os.makedirs(protobuf_classes_dir, exist_ok=True)

    schema_filename = os.path.join(protobuf_classes_dir, proto_name)
    with open(schema_filename, 'w+') as schema_f:
        schema_f.write(schema)
        schema_f.flush()

    # Compile schema to python class by protoc
    command = f"{sys.executable} -m grpc_tools.protoc -I {protobuf_classes_dir} --python_out={protobuf_classes_dir} {proto_name}"
    try:
        subprocess.run(command.split(), check=True, stdout=subprocess.PIPE, env=os.environ.copy())
    except subprocess.CalledProcessError as exc:
        raise ProtobufCompilerException(f"Cannot generate proto class: {exc}")

    # Load the class dynamically
    mod_name_with_pkg = f"tap_kafka.{mod_name}"
    loader = importlib.machinery.SourceFileLoader(
        mod_name_with_pkg, os.path.join(protobuf_classes_dir, module_filename)
    )
    spec = importlib.util.spec_from_loader(mod_name_with_pkg, loader)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    loader.exec_module(mod)

    # Get the generated class
    for name, obj in inspect.getmembers(mod):
        if inspect.isclass(obj) and obj.__module__ == module_name:
            return obj

    return None
