import os.path

from data.data import *
from utils.utils import make_dirs


class ParserBase:
    def __init__(self, db_type, dir_plot_save):
        self.db_type = db_type
        self.dir_plot_save = dir_plot_save
        make_dirs(self.dir_plot_save, reset=False)

    def parse(self):
        raise NotImplementedError


class ParserV0(ParserBase):
    """
    format: <path>
    """

    def __init__(self, db_type, addr_in, dir_plot_save):
        super().__init__(db_type, dir_plot_save)
        self.path_in = addr_in

    def parse(self):
        _basename = os.path.basename(self.path_in)
        _name, _ = os.path.splitext(_basename)
        logging.info(self.path_in)
        db_obj = eval(self.db_type)(self.path_in)
        db_obj.load()
        db_obj.plot()
