import argparse

from parsers.parser import *
from utils.utils import set_logging


def parse_args():
    parser = argparse.ArgumentParser()
    # parser.add_argument('--addr', default='/media/manu/data/cable/data_v0/neg/heta-cable.log')
    # parser.add_argument('--addr', default='/media/manu/data/cable/data_v0/neg')
    # parser.add_argument('--db_type', default='DataV0')
    # parser.add_argument('--addr', default='/media/manu/data/cable/data_v1/runtime/6号线缆/a）6号 定温报警.CSV')
    # parser.add_argument('--addr', default='/media/manu/data/cable/data_v1/runtime')
    # parser.add_argument('--db_type', default='DataV1')
    # parser.add_argument('--dir_plot_save', default='/home/manu/tmp/cable_demo_parser_save')
    # parser.add_argument('--addr',
    #                     default='/media/manu/data/cable/data_v2/隧道火灾实验数据/１、紧急停车带火灾实验/6.22紧急停车带隧道火灾实验数据/6.22实验 1L汽油 紧急停车带 风速0.3－43S报警/20240622 保护套线原始数据.CSV')
    # parser.add_argument('--addr', default='/media/manu/data/cable/data_v2/隧道火灾实验数据/３、隧道侧道火灾实验/6.25 侧壁 1L汽油 未报警')
    parser.add_argument('--addr', default='/media/manu/data/cable/data_v2/隧道火灾实验数据')
    parser.add_argument('--db_type', default='DataV1')
    parser.add_argument('--dir_plot_save', default='/home/manu/tmp/cable_demo_parser_save')
    return parser.parse_args()


def run(args):
    logging.info(args)
    # parser = ParserV0(db_type=args.db_type, addr_in=args.addr, dir_plot_save=args.dir_plot_save)
    # parser = ParserV1(db_type=args.db_type, addr_in=args.addr, dir_plot_save=args.dir_plot_save)
    # parser = ParserV0CSV(db_type=args.db_type, addr_in=args.addr, dir_plot_save=args.dir_plot_save)
    # parser = ParserV1CSV(db_type=args.db_type, addr_in=args.addr, dir_plot_save=args.dir_plot_save)
    parser = ParserV2CSV(db_type=args.db_type, addr_in=args.addr, dir_plot_save=args.dir_plot_save)
    parser.parse()


def main():
    set_logging()
    args = parse_args()
    run(args)


if __name__ == '__main__':
    main()
