import glob
import csv
import zipfile
import codecs

class Source(object):
    def get(self, ctx):
        pass

    def type(self):
        pass

class GlobSource(Source):
    def __init__(self, pattern):
        self.pattern = pattern

    def get(self, ctx):
        for fname in glob.iglob(ctx.resolve_path(self.pattern)):
            print(fname)
            yield fname

    def type(self):
        return 'file'

class FileSource(Source):
    def __init__(self, fname):
        self.fname = fname

    def get(self, ctx):
        yield ctx.resolve_file(self.fname)

    def type(self):
        return 'file'

class ZipSource(Source):
    def __init__(self, file_source):
        self.file_source = file_source
        assert file_source.type() == 'file'

    def get(self, ctx):
        print(self.file_source.get(ctx))
        for zip_file in self.file_source.get(ctx):
            print(zip_file)
            zip = zipfile.ZipFile(open(zip_file, "rb"))
            names = list(zip.namelist())
            print("%s : %d files" % (zip_file, len(names)))
            for name in names:
                f = codecs.iterdecode(zip.open(name), 'utf-8')
                yield f

    def type(self):
        return 'file'

class CSVSource(Source):
    def __init__(self, file_source, f = None, skip_header=False, fields=None):
        assert(file_source.type() == 'file')
        if skip_header and not fields:
            raise ValueError("if skip_header is True, fields must be provided")
        self.file_source = file_source
        self.fields = fields
        self.skip_header = skip_header

    def get(self, ctx):
        for f in self.file_source.get(ctx):
            rdr = csv.DictReader(f, self.fields)

            if self.skip_header:
                next(rdr)

            for row in rdr:
                yield row
