import logging
from dataclasses import dataclass, field
from datetime import datetime

import matplotlib.pyplot as plt
import numpy as np


class DataBase:
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

    def load(self):
        logging.info(self.addr)
        with open(self.addr, 'r') as f:
            _lines = f.readlines()
        logging.info(_lines)
        for _line in _lines:
            logging.info(_line)
            part_l, part_r = _line.strip().split('[')
            logging.info((part_l, part_r))
            _timestamp, _net_a, _net_b, _tmp = part_l.strip().split(' ')
            _key = _tmp
            _module_id, _cable_id, _tmp = _tmp.strip().split('.')
            _idx_start, _idx_end = _tmp.strip().split('~')
            _seq_temperature_str_lst = part_r[:-1].strip().split(',')
            _seq_temperature = [int(num) for num in _seq_temperature_str_lst]
            logging.info(
                (_timestamp, _net_a, _net_b, _module_id, _cable_id, _idx_start, _idx_end, _seq_temperature, _key))
            _new_chunk = self.Chunk()
            _new_chunk.timestamp = _timestamp
            _new_chunk.net = _net_a + '-' + _net_b
            _new_chunk.module_id = int(_module_id)
            _new_chunk.cable_id = int(_cable_id)
            _new_chunk.idx_start = int(_idx_start)
            _new_chunk.idx_end = int(_idx_end)
            _new_chunk.seq_temperature = _seq_temperature
            logging.info(_new_chunk)
            if _key not in self.db.keys():
                self.db[_key] = self.Signal()
            self.db[_key].seq_chunk.append(_new_chunk)

    def plot(self, dir_save=None, show=True, save_name=None):
        for key, signal in self.db.items():
            timestamps = []
            sequences = []
            logging.info(key)
            for chunk in signal.seq_chunk:
                timestamps.append(chunk.timestamp)
                sequences.append(chunk.seq_temperature)

            timestamps = [datetime.strptime(ts, '%H:%M:%S.%f') for ts in timestamps]
            sorted_indices = np.argsort(timestamps)
            timestamps = [timestamps[i] for i in sorted_indices]
            sequences = [sequences[i] for i in sorted_indices]

            x = np.arange(len(sequences[0]))
            y = np.arange(len(timestamps))
            x, y = np.meshgrid(x, y)
            z = np.array(sequences)

            fig = plt.figure(figsize=(12, 8))
            ax = fig.add_subplot(111, projection='3d')
            surf = ax.plot_surface(x, y, z, cmap='coolwarm', edgecolor='none')
            fig.colorbar(surf, ax=ax, shrink=0.5, aspect=5, label='Temperature')

            ax.set_xlabel('Index')
            ax.set_ylabel('Timestamp')
            ax.set_zlabel('Temperature')
            ax.set_title(f'3D Surface Plot of Sequences over Time for key {key}')
            ax.set_yticks(np.arange(len(timestamps)))
            ax.set_yticklabels([ts.strftime('%H:%M:%S') for ts in timestamps])

            if show:
                plt.show()
            if dir_save:
                save_name = f"{save_name}_{key}.png"
                plt.savefig(f"{dir_save}/{save_name}")
            plt.close(fig)
