# © Copyright Databand.ai, an IBM Company 2022

import os

from os.path import normpath
from typing import Optional

import attr
import six

from targets.utils.path import trailing_slash


class FileCompressions(object):
    none = "none"
    gzip = "gz"
    bzip = "bz2"
    zip = "zip"
    xz = "xz"
    snappy = "snappy"


class FileFormat(object):
    csv = "csv"
    txt = "txt"
    hdf5 = "h5"
    json = "json"
    table = "table"
    feather = "feather"
    numpy = "npy"
    jpeg = "jpeg"
    png = "png"
    pdf = "pdf"
    excel = "xlsx"
    parquet = "parquet"
    html = "html"
    pickle = "pickle"
    yaml = "yaml"
    tsv = "tsv"
    tfmodel = "tfmodel"
    tfhistory = "tfhistory"
    zip = "zip"


BINARY_FORMATS = [
    FileFormat.hdf5,
    FileFormat.feather,
    FileFormat.numpy,
    FileFormat.excel,
    FileFormat.parquet,
    FileFormat.pickle,
    FileFormat.tfmodel,
]


@attr.s(frozen=True, repr=True)
class _FileExtensionMap(object):
    _ext_to_name = attr.ib(factory=dict)
    _name_to_ext = attr.ib(factory=dict)

    def register_extension(self, name, extensions=None):
        extensions = extensions or name
        if isinstance(extensions, six.string_types):
            extensions = [extensions]
        for e in extensions:
            self._ext_to_name[e] = name
        self._name_to_ext[name] = extensions

    def get_default_extension(self, name):
        if not name:
            return ""

        if name in self._name_to_ext:
            ext = self._name_to_ext[name][0]
            return ".%s" % ext if ext else ""

        # default fallback
        return ".%s" % name

    def known_ext(self, name):
        return name in self._ext_to_name

    def extension_to_name(self, extention):
        return self._ext_to_name.get(extention)


KNOWN_FORMATS = _FileExtensionMap()
KNOWN_COMPRESSIONS = _FileExtensionMap()


def register_file_extension(name, extra_extensions=None):
    KNOWN_FORMATS.register_extension(name, extra_extensions)
    return name


def register_compression(name, ext=None):
    KNOWN_COMPRESSIONS.register_extension(name, ext)


register_compression(FileCompressions.gzip, "gz")
register_compression(FileCompressions.bzip, "bz2")
register_compression(FileCompressions.zip)
register_compression(FileCompressions.xz)
register_compression(FileCompressions.snappy)

register_file_extension(FileFormat.csv)
register_file_extension(FileFormat.txt, ["txt", "py", "log"])
register_file_extension(FileFormat.table)
register_file_extension(FileFormat.parquet)
register_file_extension(FileFormat.feather)
register_file_extension(FileFormat.excel)
register_file_extension(FileFormat.pickle)
register_file_extension(FileFormat.html)
register_file_extension(FileFormat.png)
register_file_extension(FileFormat.pdf)
register_file_extension(FileFormat.jpeg, ["jpg", "jpeg"])
register_file_extension(FileFormat.hdf5, ["h5", "hdf5"])
register_file_extension(FileFormat.json, ["json", "hjson"])
register_file_extension(FileFormat.yaml, ["yaml", "yml"])
register_file_extension(FileFormat.numpy, ["npy", "numpy"])
register_file_extension(FileFormat.tsv)
register_file_extension(FileFormat.tfmodel)
register_file_extension(FileFormat.tfhistory)
register_file_extension(FileFormat.zip)


@attr.s(frozen=True, repr=False)
class TargetConfig(object):
    format = attr.ib(default=None)  # type: Optional[str]
    compression = attr.ib(default=None)  # type: Optional[str]
    folder = attr.ib(default=False)  # type: bool
    meta_files = attr.ib(default=tuple())

    flag = attr.ib(default=True)
    is_binary = attr.ib(default=False)
    target_factory = attr.ib(default=None)
    require_local_access = attr.ib(default=False)
    overwrite_target = attr.ib(default=False)

    def with_compression(self, compression):
        return attr.evolve(self, compression=compression)

    def with_format(self, format):
        # type: (TargetConfig, str) -> TargetConfig
        return attr.evolve(self, format=format, is_binary=format in BINARY_FORMATS)

    def with_meta_files(self, list_of_meta_files):
        return attr.evolve(self, meta_files=list_of_meta_files, folder=True)

    def with_flag(self, flag):
        # type: (TargetConfig, bool) -> TargetConfig
        return attr.evolve(self, flag=flag)

    def without_flag(self):
        return attr.evolve(self, flag=None)

    def with_target_factory(self, target_factory):
        return attr.evolve(self, target_factory=target_factory)

    def as_folder(self):
        return attr.evolve(self, folder=True)

    def as_file(self):
        return attr.evolve(self, folder=False, flag=None, target_factory=None)

    def with_require_local_access(self):
        return attr.evolve(self, require_local_access=True)

    def with_overwrite(self):
        return attr.evolve(self, overwrite_target=True)

    @property
    def gzip(self):
        return self.with_compression(FileCompressions.gzip)

    @property
    def gz(self):
        return self.gzip

    @property
    def bzip(self):
        return self.with_compression(FileCompressions.bzip)

    @property
    def snappy(self):
        return self.with_compression(FileCompressions.snappy)

    @property
    def zip(self):
        return self.with_format(FileFormat.zip)

    @property
    def xz(self):
        return self.with_compression(FileCompressions.xz)

    @property
    def csv(self):
        return self.with_format(FileFormat.csv)

    @property
    def tsv(self):
        return self.with_format(FileFormat.tsv)

    @property
    def txt(self):
        return self.with_format(FileFormat.txt)

    @property
    def hdf5(self):
        return self.with_format(FileFormat.hdf5)

    @property
    def json(self):
        return self.with_format(FileFormat.json)

    @property
    def feather(self):
        return self.with_format(FileFormat.feather)

    @property
    def numpy(self):
        return self.with_format(FileFormat.numpy)

    @property
    def excel(self):
        return self.with_format(FileFormat.excel)

    @property
    def parquet(self):
        return self.with_format(FileFormat.parquet)

    @property
    def pickle(self):
        return self.with_format(FileFormat.pickle)

    @property
    def tfmodel(self):
        return self.with_format(FileFormat.tfmodel)

    @property
    def tfhistory(self):
        return self.with_format(FileFormat.tfhistory)

    @property
    def overwrite(self):
        return self.with_overwrite()

    def get_ext(self):  # type: (TargetConfig) -> str
        ext = ""
        if self.format:
            ext += KNOWN_FORMATS.get_default_extension(self.format)

        if self.format not in [
            file.parquet,
            file.hdf5,
        ]:  # we don't add compression to this formats
            ext += KNOWN_FORMATS.get_default_extension(self.compression)
        return ext

    def __repr__(self):
        ext = self.get_ext()
        if self.folder:
            ext += "/"
        return ext


file = TargetConfig()
folder = TargetConfig().as_folder()


def parse_target_config(config_str):
    if not config_str.startswith("."):
        config_str = ".%s" % config_str
    return extract_target_config_from_path(config_str)


def extract_target_config_from_path(path, config=None):
    # type: (str, TargetConfig)-> TargetConfig
    config = config or TargetConfig()
    if path is None:
        return config
    if path.endswith("[noflag]"):
        config = config.with_flag(None)
        path = path[:-8]

    if trailing_slash(path):
        # we move to folder mode regardles config
        config = config.as_folder()
        path = os.path.dirname(path)

    path = normpath(path)
    if config.format or config.compression:
        # we don't set "forced" settings
        return config
    path = os.path.basename(path)

    # Infer format and compression from the filename/URL extension
    parts = path.split(".")
    if len(parts) > 2:
        parts = parts[-2:]
    for p in parts:
        # priority to last elements
        # for example  parquet.csv is csv
        format = KNOWN_FORMATS.extension_to_name(p)
        if format:
            config = config.with_format(format)
        else:
            compression = KNOWN_COMPRESSIONS.extension_to_name(p)
            if compression:
                config = config.with_compression(compression)
    return config
