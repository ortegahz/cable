import argparse

from parsers.parser import *
from utils.utils import set_logging


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--addr', default='/media/manu/data/cable/data_v3/fire-alarm/heta-cable.2024-05-24.0.log')
    # parser.add_argument('--addr', default='/media/manu/data/cable/data_v3/fire-alarm/')
    parser.add_argument('--db_type', default='DataV0')
    # parser.add_argument('--addr', default='/home/manu/tmp/cable_demo_parser_save_v2/heta-cable.2024-06-16.0_segment_2.csv')
    # parser.add_argument('--addr', default='/home/manu/tmp/cable_demo_parser_save_v2')
    # parser.add_argument('--db_type', default='DataV1')
    # parser.add_argument('--dir_plot_save', default='/home/manu/tmp/cable_demo_parser_save')
    # parser.add_argument('--addr',
    #                     default='/media/manu/data/cable/data_v2/隧道火灾实验数据/３、隧道侧道火灾实验/6.24侧壁 1L汽油 小火盆 侧壁电缆报警/27s后点火 侧壁 53s报警 位置：567~571  20240624.CSV')
    # parser.add_argument('--addr', default='/media/manu/data/cable/data_v2/隧道火灾实验数据/３、隧道侧道火灾实验/6.25 侧壁 1L汽油 未报警')
    # parser.add_argument('--addr', default='/media/manu/data/cable/data_v2/隧道火灾实验数据')
    # parser.add_argument('--db_type', default='DataV1')
    parser.add_argument('--dir_plot_save', default='/home/manu/tmp/cable_demo_parser_save')
    return parser.parse_args()


def run(args):
    logging.info(args)
    parser = ParserV0(db_type=args.db_type, addr_in=args.addr, dir_plot_save=args.dir_plot_save)
    # parser = ParserV1(db_type=args.db_type, addr_in=args.addr, dir_plot_save=args.dir_plot_save)
    # parser = ParserV0CSV(db_type=args.db_type, addr_in=args.addr, dir_plot_save=args.dir_plot_save)
    # parser = ParserV1CSV(db_type=args.db_type, addr_in=args.addr, dir_plot_save=args.dir_plot_save)
    # parser = ParserV2CSV(db_type=args.db_type, addr_in=args.addr, dir_plot_save=args.dir_plot_save)
    parser.parse()


def main():
    set_logging()
    args = parse_args()
    run(args)


if __name__ == '__main__':
    main()
