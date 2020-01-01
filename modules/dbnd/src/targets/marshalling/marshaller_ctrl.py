import types

import attr

from dbnd._core.errors import friendly_error
from targets.marshalling import StrLinesMarshaller, StrMarshaller
from targets.marshalling.marshaller import Marshaller
from targets.values import ValueType


@attr.s
class MarshallerCtrl(object):
    target = attr.ib()
    marshaller = attr.ib()  # type: Marshaller
    value_type = attr.ib()  # type: ValueType

    def load(self, **kwargs):
        value = self._load(**kwargs)
        if isinstance(self.marshaller, StrMarshaller):
            value = self.value_type.parse_from_str(value)
        elif isinstance(self.marshaller, StrLinesMarshaller):
            value = [line.rstrip("\n") for line in value]
            value = self.value_type.parse_from_str_lines(value)
        return value

    def _load(self, **kwargs):
        # dir target
        from targets.dir_target import DirTarget
        from targets.file_target import FileTarget
        from targets.multi_target import MultiTarget

        m = self.marshaller
        target = self.target
        if isinstance(target, MultiTarget):
            if m.support_multi_target_direct_read:
                return m.target_to_value(target, **kwargs)
        elif isinstance(target, DirTarget):
            if m.support_directory_direct_read and m.support_direct_access(target):
                return m.target_to_value(target, **kwargs)
        elif isinstance(target, FileTarget):
            return m.target_to_value(target, **kwargs)

        partitions = target.list_partitions()
        if len(partitions) == 1:
            return m.target_to_value(partitions[0], **kwargs)

        if not self.value_type.support_merge:
            raise friendly_error.marshaller_no_merge(self, target, partitions)
        # Concatenate all data into one DataFrame
        # We don't want list to be stored in memory
        # however, concat does list() on the iterator as one of the first things
        partitions_values = [m.target_to_value(t, **kwargs) for t in partitions]
        return self.value_type.merge_values(*partitions_values)

    def dump(self, value, **kwargs):
        target = self.target
        from targets.multi_target import MultiTarget

        from targets.marshalling import StrMarshaller, StrLinesMarshaller

        if isinstance(self.marshaller, StrMarshaller):
            mode = kwargs.get("mode")
            if not mode or "b" not in mode:
                # TODO: sometimes we write to strmarshaller binary format.
                value = self.value_type.to_str(value)
        elif isinstance(self.marshaller, StrLinesMarshaller):
            value = self.value_type.to_str_lines(value)
            kwargs["newline"] = kwargs.pop("newline", True)

        m = self.marshaller
        if isinstance(target, MultiTarget):
            raise friendly_error.targets.dump_to_multi_target(self, value)

        from targets.dir_target import DirTarget

        if isinstance(value, types.GeneratorType):
            if not isinstance(target, DirTarget):
                raise friendly_error.targets.dump_generator_to_file(self)
            for value_partition in value:
                m.value_to_target(
                    target=target.partition(), value=value_partition, **kwargs
                )

            target.mark_success()
            return
        selected_target = target
        if isinstance(target, DirTarget):
            # WRITE TO FILE INSIDE DIR.
            if not m.support_directory_direct_write:
                selected_target = target._write_target
        m.value_to_target(target=selected_target, value=value, **kwargs)
        target.mark_success()

    def load_partitioned(self, **kwargs):
        for t in self.target.list_partitions():
            yield self.marshaller.target_to_value(t, **kwargs)
