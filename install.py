import os

def add_cammand(cammand):
    with open(os.path.expanduser("~/.bashrc"), 'r') as f:
        data = f.read()

    for l in data.split('\n'):
        if l.strip() == cammand:
            break
    else:
        with open(os.path.expanduser("~/.bashrc"), 'a') as f:
            print('\n' + cammand, file=f, end='')

if __name__ == '__main__':
    
    path = os.path.abspath(os.path.join(os.curdir, 'sapp.py'))
    cammand = f"alias sapp='python {path}'"
    add_cammand(cammand)

    cammand = "alias spython='sapp python'"
    add_cammand(cammand)