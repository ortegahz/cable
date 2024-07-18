import csv
import io
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from datetime import timedelta

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.animation import FuncAnimation
from scipy.stats import mode


class DataBase:
    _cnt = 0

    def __init__(self):
        self.db = dict()


class DataV0(DataBase):
    @dataclass
    class Chunk:
        timestamp: str = ''
        net: str = ''
        module_id: int = -1
        cable_id: int = -1
        idx_start: int = -1
        idx_end: int = -1
        seq_temperature: list = field(default_factory=list)

    @dataclass
    class Signal:
        seq_chunk: list = field(default_factory=list)

    def __init__(self, addr):
        super().__init__()
        self.addr = addr
        self.time_templet = '%H:%M:%S.%f'

    def _chunk_merge(self, time_delta=10):
        _keys = list(self.db.keys())
        for _key in _keys:
            logging.info(f'_key --> {_key}')
            _idx_end_max = 0
            all_temperatures = []
            for chunk in self.db[_key].seq_chunk:
                _idx_end_max = chunk.idx_end if chunk.idx_end > _idx_end_max else _idx_end_max
                all_temperatures.extend(chunk.seq_temperature)
            logging.info(f'_idx_end_max --> {_idx_end_max}')

            mode_temperature = mode(all_temperatures).mode[0] if all_temperatures else 0
            logging.info(f'Mode of temperatures --> {mode_temperature}')

            _key_new = _key + '_all'
            self.db[_key_new] = self.Signal()
            all_chunks = sorted(self.db[_key].seq_chunk,
                                key=lambda chunk: datetime.strptime(chunk.timestamp, self.time_templet))
            start_time = datetime.strptime(all_chunks[0].timestamp, self.time_templet)
            end_time = datetime.strptime(all_chunks[-1].timestamp, self.time_templet)
            current_time = start_time

            _previous_temperatures = [mode_temperature] * (_idx_end_max + 1)
            chunk_index = 0

            while current_time <= end_time:
                if chunk_index < len(all_chunks) and \
                        datetime.strptime(all_chunks[chunk_index].timestamp, self.time_templet) <= \
                        current_time + timedelta(seconds=time_delta):
                    _chunk = all_chunks[chunk_index]
                    _chunk_ff_temperatures = _previous_temperatures[:]
                    assert (_chunk.idx_end + 1 - _chunk.idx_start == len(_chunk.seq_temperature))
                    _chunk_ff_temperatures[_chunk.idx_start:_chunk.idx_end + 1] = _chunk.seq_temperature
                    _previous_temperatures = _chunk_ff_temperatures[:]
                    chunk_index += 1
                else:
                    _chunk_ff_temperatures = _previous_temperatures[:]

                _chunk_new = self.Chunk(
                    timestamp=current_time.strftime(self.time_templet),
                    net='',
                    module_id=-1,
                    cable_id=-1,
                    idx_start=-1,
                    idx_end=-1,
                    seq_temperature=_chunk_ff_temperatures)

                self.db[_key_new].seq_chunk.append(_chunk_new)
                current_time += timedelta(seconds=time_delta)

    def load(self):
        # logging.info(self.addr)
        with open(self.addr, 'r') as f:
            _lines = f.readlines()
        # logging.info(_lines)
        for _line in _lines:
            # logging.info(_line)
            part_l, part_r = _line.strip().split('[')
            # logging.info((part_l, part_r))
            _timestamp, _net_a, _net_b, _tmp = part_l.strip().split(' ')
            _module_id, _cable_id, _tmp = _tmp.strip().split('.')
            _key = _cable_id
            _idx_start, _idx_end = _tmp.strip().split('~')
            _seq_temperature_str_lst = part_r[:-1].strip().split(',')
            _seq_temperature = [int(num) for num in _seq_temperature_str_lst]
            # _seq_temperature = [512 if x == -97 else x for x in _seq_temperature]  # diff alarm
            # _seq_temperature = [1024 if x == -98 else x for x in _seq_temperature]  # const alarm
            # _seq_temperature = [0 if x < 0 else x for x in _seq_temperature]
            # _seq_temperature_warning = [x for x in _seq_temperature if x < 0]
            # if len(_seq_temperature_warning) > 0:
            #     logging.info(f'_seq_temperature_warning --> {_seq_temperature_warning}')
            # logging.info(
            #     (_timestamp, _net_a, _net_b, _module_id, _cable_id, _idx_start, _idx_end, _seq_temperature, _key))
            _new_chunk = self.Chunk()
            _new_chunk.timestamp = _timestamp
            _new_chunk.net = _net_a + '-' + _net_b
            _new_chunk.module_id = int(_module_id)
            _new_chunk.cable_id = int(_cable_id)
            _new_chunk.idx_start = int(_idx_start)
            _new_chunk.idx_end = int(_idx_end)
            _new_chunk.seq_temperature = _seq_temperature
            # logging.info(_new_chunk)
            if _key not in self.db.keys():
                self.db[_key] = self.Signal()
            self.db[_key].seq_chunk.append(_new_chunk)
        self._chunk_merge()

    def plot(self, dir_save=None, show=True, save_name_prefix=None):
        for key, signal in self.db.items():
            if 'all' not in key:
                continue
            timestamps = []
            sequences = []
            logging.info(key)
            for chunk in signal.seq_chunk:
                # logging.info(f'chunk.timestamp --> {chunk.timestamp}')
                # logging.info(f'chunk.seq_temperature --> {chunk.seq_temperature}')
                timestamps.append(chunk.timestamp)
                sequences.append(chunk.seq_temperature)

            timestamps = [datetime.strptime(ts, self.time_templet) for ts in timestamps]
            sorted_indices = np.argsort(timestamps)
            timestamps = [timestamps[i] for i in sorted_indices]
            sequences = [sequences[i] for i in sorted_indices]

            if len(timestamps) < 2 or len(sequences[0]) < 2:
                logging.warning(f"Not enough data points to plot for key {key}")
                continue

            logging.info(f'DataV0._cnt --> {DataV0._cnt}')

            x = np.arange(len(sequences[0]))
            y = np.arange(len(timestamps))
            x, y = np.meshgrid(x, y)
            z = np.array(sequences)

            fig = plt.figure(figsize=(12, 8))
            ax = fig.add_subplot(111, projection='3d')
            # _res = ax.plot_surface(x, y, z, cmap='coolwarm', edgecolor='none', vmin=0, vmax=100)
            # _res = ax.plot_wireframe(x, y, z, color='blue')
            _res = ax.scatter(x, y, z)
            fig.colorbar(_res, ax=ax, shrink=0.5, aspect=5, label='Temperature')

            ax.set_zlim(0, 100)
            ax.set_zticks(np.arange(0, 101, 10))

            ax.set_xlabel('Index')
            ax.set_ylabel('Timestamp')
            ax.set_zlabel('Temperature')
            ax.set_title(f'3D Surface Plot of Sequences over Time for key {key}')
            ax.set_yticks(np.arange(len(timestamps)))
            ax.set_yticklabels([ts.strftime('%H:%M:%S') for ts in timestamps])

            if show:
                plt.show()
            if dir_save:
                save_name = f'{DataV0._cnt}_{save_name_prefix}_{key}.png'
                plt.savefig(os.path.join(dir_save, save_name))
                DataV0._cnt += 1
            plt.close(fig)

    def plot_animation(self, window_size=10):
        for key, signal in self.db.items():
            if 'all' not in key:
                continue
            timestamps = []
            sequences = []
            logging.info(key)
            for chunk in signal.seq_chunk:
                timestamps.append(chunk.timestamp)
                sequences.append(chunk.seq_temperature)

            timestamps = [datetime.strptime(ts, self.time_templet) for ts in timestamps]
            sorted_indices = np.argsort(timestamps)
            timestamps = [timestamps[i] for i in sorted_indices]
            sequences = [sequences[i] for i in sorted_indices]

            if len(timestamps) < 2 or len(sequences[0]) < 2:
                logging.warning(f"Not enough data points to plot for key {key}")
                continue

            logging.info(f'DataV0._cnt --> {DataV0._cnt}')

            x = np.arange(len(sequences[0]))
            y = np.arange(len(timestamps))
            x, y = np.meshgrid(x, y)
            z = np.array(sequences)

            fig = plt.figure(figsize=(12, 8))
            ax = fig.add_subplot(111, projection='3d')

            def update_plot(frame):
                ax.clear()
                start_idx = frame
                end_idx = start_idx + window_size
                if end_idx > len(timestamps):
                    end_idx = len(timestamps)
                _x = x[start_idx:end_idx]
                _y = y[start_idx:end_idx]
                _z = z[start_idx:end_idx, :]
                _res = ax.scatter(_x, _y, _z)
                ax.set_zlim(0, 100)
                ax.set_zticks(np.arange(0, 101, 10))
                ax.set_xlabel('Index')
                ax.set_ylabel('Timestamp')
                ax.set_zlabel('Temperature')
                ax.set_title(f'3D Scatter Plot of Sequences over Time for key {key}')
                ax.set_yticks(np.arange(start_idx, end_idx))
                ax.set_yticklabels([timestamps[i].strftime('%H:%M:%S') for i in range(start_idx, end_idx)])
                return _res,

            num_frames = len(timestamps) - window_size + 1
            anim = FuncAnimation(fig, update_plot, frames=num_frames, blit=False, repeat=False)
            plt.show()
            plt.close(fig)

    def _split_chunks(self, signal):
        new_chunks = []
        all_temperatures = [temp for chunk in signal.seq_chunk for temp in chunk.seq_temperature]
        mode_temperature = mode(all_temperatures).mode[0] if all_temperatures else 0

        for chunk in signal.seq_chunk:
            temperatures = chunk.seq_temperature
            for i in range(0, len(temperatures), 64):
                chunk_temperatures = temperatures[i:i + 64]
                if len(chunk_temperatures) < 64:
                    chunk_temperatures.extend([mode_temperature] * (64 - len(chunk_temperatures)))
                new_chunk = self.Chunk(
                    timestamp=chunk.timestamp,
                    net=chunk.net,
                    module_id=i // 64,
                    cable_id=chunk.cable_id,
                    idx_start=i,
                    idx_end=i + 63,
                    seq_temperature=chunk_temperatures
                )
                new_chunks.append(new_chunk)
        return new_chunks

    def save_to_csv(self, csv_dir, case_name, time_delta=16):
        fieldnames = ['date1', 'time1', 'date2', 'time2', 'group', 'idx_start'] + [
            f'temperature_{i}' for i in range(64)]

        for key, signal in self.db.items():
            if 'all' not in key:
                continue

            chunks = self._split_chunks(signal)
            start_time = None
            end_time = None
            csv_file_index = 0
            writer = None
            csvfile = None

            for chunk in chunks:
                chunk_time = datetime.strptime(chunk.timestamp, self.time_templet)

                if start_time is None:
                    start_time = chunk_time
                    end_time = start_time + timedelta(seconds=time_delta)
                    csv_file_name = os.path.join(csv_dir, f'{case_name}_segment_{csv_file_index}.csv')
                    csvfile = open(csv_file_name, 'w', newline='')
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    # writer.writeheader()

                if chunk_time >= end_time:
                    csvfile.close()
                    csv_file_index += 1
                    start_time = chunk_time
                    end_time = start_time + timedelta(seconds=time_delta)
                    csv_file_name = os.path.join(csv_dir, f'{case_name}_segment_{csv_file_index}.csv')
                    csvfile = open(csv_file_name, 'w', newline='')
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    # writer.writeheader()

                row = {
                    'date1': '24-07-16',
                    'time1': chunk.timestamp,
                    'date2': '24-07-16',
                    'time2': chunk.timestamp,
                    'group': f'[{chunk.module_id}]',
                    'idx_start': chunk.idx_start,
                }
                row.update({f'temperature_{i}': chunk.seq_temperature[i] for i in range(64)})
                writer.writerow(row)

            if csvfile:
                csvfile.close()


class DataV1(DataV0):
    @dataclass
    class Chunk:
        timestamp: str = ''
        group_id: int = -1
        idx_s: int = -1
        seq_temperature: list = field(default_factory=list)

    @dataclass
    class Signal:
        seq_chunk: list = field(default_factory=list)

    def __init__(self, addr):
        super().__init__(addr)
        self.time_templet = '%H:%M:%S.%f'

    def _chunk_merge(self):
        _keys = list(self.db.keys())
        for _key in _keys:
            logging.info(f'_key --> {_key}')
            _idx_start_max = 0
            for chunk in self.db[_key].seq_chunk:
                _idx_start_max = chunk.idx_s if chunk.idx_s > _idx_start_max else _idx_start_max
            logging.info(f'_idx_start_max --> {_idx_start_max}')
            _key_new = _key + '_all'
            self.db[_key_new] = self.Signal()
            all_chunks = []
            all_chunks.extend(self.db[_key].seq_chunk)
            sorted_chunks = sorted(all_chunks, key=lambda chunk: datetime.strptime(chunk.timestamp, self.time_templet))

            current_chunk = None
            current_timestamp = None

            for _chunk in sorted_chunks:
                # logging.info(f'_chunk.timestamp --> {_chunk.timestamp}')
                # logging.info(f'_chunk.idx_s --> {_chunk.idx_s}')
                # logging.info(f'_chunk.seq_temperature --> {_chunk.seq_temperature}')

                if current_timestamp is None or _chunk.timestamp != current_timestamp:
                    if current_chunk is not None:
                        self.db[_key_new].seq_chunk.append(current_chunk)
                        # logging.info(f'current_chunk.timestamp --> {current_chunk.timestamp}')
                        # logging.info(f'current_chunk.idx_s --> {current_chunk.idx_s}')
                        # logging.info(f'current_chunk.seq_temperature --> {current_chunk.seq_temperature}')

                    current_chunk = self.Chunk(
                        timestamp=_chunk.timestamp,
                        group_id=_chunk.group_id,
                        idx_s=_chunk.idx_s,
                        seq_temperature=[0] * (_idx_start_max + 64 - 1)
                    )
                    current_timestamp = _chunk.timestamp

                _chunk.idx_s = 1 if _chunk.idx_s == 0 else _chunk.idx_s  # for data compatibility
                current_chunk.seq_temperature[_chunk.idx_s - 1:_chunk.idx_s - 1 + 64] = _chunk.seq_temperature

            if current_chunk is not None:
                self.db[_key_new].seq_chunk.append(current_chunk)
                # logging.info(f'current_chunk.timestamp --> {current_chunk.timestamp}')
                # logging.info(f'current_chunk.idx_s --> {current_chunk.idx_s}')
                # logging.info(f'current_chunk.seq_temperature --> {current_chunk.seq_temperature}')

    @staticmethod
    def _remove_nul_bytes(input_file):
        with open(input_file, 'rb') as f:
            content = f.read().replace(b'\x00', b'')
        return content

    def load(self):
        cleaned_content = self._remove_nul_bytes(self.addr)
        cleaned_file = io.StringIO(cleaned_content.decode('ISO-8859-1'))
        with cleaned_file as file:
            reader = csv.reader(file, delimiter='\t')
            for row in reader:
                row_lst = row[0].strip().split(',')
                # logging.info(row_lst)
                if len(row_lst) < 6:
                    continue
                timestamp = row_lst[1]
                if not re.search(r'\[.*\]', row_lst[4]):
                    return -1
                _group_id = int(row_lst[4].strip("[] ").strip())
                _idx_s = int(row_lst[5])
                _key = '-1'  # fake cable id, -1 stand for un-known
                temperatures = \
                    [int(temp) if 'ALARM:' not in temp else int(temp.replace('ALARM:', '')) for temp in row_lst[6:]]
                # temperatures = [0 if x < 0 else x for x in temperatures]
                # logging.info((timestamp, _idx_s, temperatures))
                chunk = self.Chunk(timestamp=timestamp, group_id=_group_id, idx_s=_idx_s, seq_temperature=temperatures)
                if _key not in self.db.keys():
                    self.db[_key] = self.Signal()
                self.db[_key].seq_chunk.append(chunk)
            self._chunk_merge()
        return 0
