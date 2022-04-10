#! /usr/bin/env python
from other import *
from gpustat import get_card_list
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

        card_list = get_card_list()

        # Step 1: queue (-p)
        title = 'Step 1/6: Please select the queue you want to use:'
        options = [(k, str(sum(v.values()))) for k, v in card_list.items()]
        option, _ = pick(options, get_title(title), indicator='=>', options_map_func=lambda x: f"{x[0]} (Avail: {x[1]})")
        args.extend(['-p', option[0]])

        # Step 2: GPU type (--gres)
        title = 'Step 2/6: Please select the type of GPUs:'
        options = [('Any type', str(sum(card_list[option[0]].values())))] + [(k, str(v)) for k, v in card_list[option[0]].items()]
        gpu, _ = pick(options, get_title(title), indicator='=>', options_map_func=lambda x: f"{x[0]} (Avail: {x[1]})")
        avail = gpu[1]
        gpu = 'gpu' if gpu[0] == 'Any type' else 'gpu:' + gpu[0]

        # Step 3: # of GPUs (--gres)
        title = f'Step 3/6: Please select the number of GPUs you need (Avail: {avail}):'
        options = ['1', '2', '4', '8']
        num, _ = opick(options, get_title(title), indicator='=>', verify='number')
        if not num: num = '1'
        args.extend([f"'--gres={gpu}:{num}'"])

        # Step 4: # of CPUs (--cpus-per-task)
        title = f'Step 4/6: Please select the number of CPUs you need:'
        options = ['Default', '1', '2', '4', '6']
        num, _ = opick(options, get_title(title), indicator='=>', verify='number')
        if num != 'Default': args.extend([f'--cpus-per-task={num}'])

        # Step 5: time (--time)
        title = 'Step 5/6: Please input the time require to finish the job (in minutes):'
        answer = fill(get_title(title), verify='number')
        if not answer: answer = '60'
        args.extend([f'--time={answer}'])

        # Step 6: confirm
        title = 'Step 6/6: Please confirm your cammand:'
        answer, idx = pick(["Yes, submit it.", "No, exit and I'll make some changes."], get_title(title), indicator='=>', default_index=1)
        if idx != 0: exit(0)

        return args

if __name__ == '__main__':
    sapp = Sapp()
    args = sapp.get_args()
    os.system(' '.join(args + sys.argv[1:]))