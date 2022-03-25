#! /usr/bin/env python
from other import *
import os, sys, json

class Sapp():
    def __init__(self) -> None:
        self.path = os.path.expanduser("~/.sapp")

    def read_config(self):
        if os.path.exists(self.path):
            with open(self.path, 'r') as f:
                config = json.loads(f.read())
        else:
            config = {}
        return config

    def save_config(self, config):
        with open(self.path, 'w') as f:
            print(json.dumps(config), file=f)
        
    def get_args(self):

        config = self.read_config()
        options = list(config.keys())

        # Step 0: name
        title = 'Welcome to use sapp! Please select the name of the config:'
        name, _ = opick(options, title, hint='Create new config', default='temp', indicator='=>')

        if name in config:
            args = config[name]
        else:
            args = self.make_config()
            config[name] = args
            if name != 'temp': self.save_config(config)
        
        return args

    def make_config(self):

        args = ['srun', '-N', '1', '-X']

        def get_title(title):
            return '# Current command:\n#\n#    $ ' + ' '.join(args) + ' ...\n#\n' + title

        # Step 1: queue (-p)
        title = 'Step 1/4: Please select the queue you want to use:'
        options = ['debug', 'critical', 'normal']
        option, _ = pick(options, get_title(title), indicator='=>')
        args.extend(['-p', option])

        # Step 2: GPU type (--gres)
        title = 'Step 2/4: Please select the type of GPUs:'
        options = ['Any type', 'NVIDIAA40', 'NVIDIAGeForceRTX2080Ti', 'NVIDIATITANRTX', 'TeslaV100-PCIE-16GB', 'NVIDIATITANXp', 'NVIDIAGeForceGTX1080', 'NVIDIATITANV', 'TeslaM4024GB', 'NVIDIATITANXPascal)']
        gpu, _ = pick(options, get_title(title), indicator='=>')
        gpu = 'gpu' if gpu == 'Any type' else 'gpu:' + gpu

        # Step 3: # of GPUs (--gres)
        title = 'Step 3/4: Please select the number of GPUs you need:'
        options = ['1', '2', '4', '8']
        num, _ = opick(options, get_title(title), indicator='=>', verify='number')
        if not num: num = '1'
        args.extend([f'--gres={gpu}:{num}'])

        # Step 4: time (--time)
        title = 'Step 4/4: Please input the time require to finish the job (in minutes):'
        answer = fill(get_title(title), verify='number')
        if not answer: answer = '60'
        args.extend([f'--time={answer}'])

        return args

if __name__ == '__main__':
    sapp = Sapp()
    args = sapp.get_args()
    os.system(' '.join(args + sys.argv[1:]))