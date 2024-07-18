import argparse

from parsers.parser import *
from utils.utils import set_logging, make_dirs


def parse_log_line(line):
    parts = line.split()
    timestamp_str = parts[0]
    timestamp = datetime.strptime(timestamp_str, "%H:%M:%S.%f")
    return timestamp, line


def split_log_file(input_file, output_dir, max_interval_minutes=20):
    _basename = os.path.basename(input_file)
    _name, _ = os.path.splitext(_basename)

    with open(input_file, 'r') as file:
        lines = file.readlines()

    if not lines:
        print("The log file is empty.")
        return

    file_count = 0
    current_file_lines = []
    timestamp_last = None

    for line in lines:
        timestamp, line = parse_log_line(line)

        if timestamp_last is None:
            timestamp_last = timestamp

        if (timestamp - timestamp_last) > timedelta(minutes=max_interval_minutes):
            output_file = os.path.join(output_dir, f"{_name}_{file_count}.txt")
            with open(output_file, 'w') as output:
                output.writelines(current_file_lines)
            file_count += 1
            current_file_lines = []

        current_file_lines.append(line)
        timestamp_last = timestamp

    if current_file_lines:
        output_file = os.path.join(output_dir, f"{_name}_{file_count}.txt")
        with open(output_file, 'w') as output:
            output.writelines(current_file_lines)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--addr_in', default='/media/manu/data/cable/data_v3/fire-alarm/')
    parser.add_argument('--addr_out', default='/home/manu/tmp/split_logs')
    parser.add_argument('--ext', default='log')
    parser.add_argument('--max_interval_minutes', default=10)
    return parser.parse_args()


def run(args):
    logging.info(args)
    make_dirs(args.addr_out, reset=True)
    _paths = glob.glob(os.path.join(args.addr_in, f'*.{args.ext}'))
    for i, _path in enumerate(_paths):
        # if 'heta-cable.2024-05-24.0' not in _path:
        #     continue
        logging.info(f'{i + 1}th/{len(_paths)} --> {_path}')
        split_log_file(_path, args.addr_out, max_interval_minutes=args.max_interval_minutes)


def main():
    set_logging()
    args = parse_args()
    run(args)


if __name__ == '__main__':
    main()
