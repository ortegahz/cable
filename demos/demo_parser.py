import argparse

from parsers.parser import *
from utils.utils import set_logging


def parse_args():
    parser = argparse.ArgumentParser()
    # parser.add_argument('--addr', default='/media/manu/data/cable/data_v0/neg/heta-cable.2024-05-24.0.log')
    # parser.add_argument('--addr', default='/media/manu/data/cable/data_v0/neg')
    # parser.add_argument('--db_type', default='DataV0')
    parser.add_argument('--addr', default='/media/manu/data/cable/data_v1/runtime/6号线缆/a）6号 定温报警.CSV')
    # parser.add_argument('--addr', default='/media/manu/data/cable/data_v1/runtime')
    parser.add_argument('--db_type', default='DataV1')
    parser.add_argument('--dir_plot_save', default='/home/manu/tmp/cable_demo_parser_save')
    return parser.parse_args()


def run(args):
    logging.info(args)
    parser = ParserV0(db_type=args.db_type, addr_in=args.addr, dir_plot_save=args.dir_plot_save)
    # parser = ParserV1CSV(db_type=args.db_type, addr_in=args.addr, dir_plot_save=args.dir_plot_save)
    parser.parse()


def main():
    set_logging()
    args = parse_args()
    run(args)


if __name__ == '__main__':
    main()
