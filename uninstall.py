import os

def remove_cammand(cammand):
    with open(os.path.expanduser("~/.bashrc"), 'r') as f:
        data = f.read()

    new_data = []
    for l in data.split('\n'):
        if l.strip() != cammand:
            new_data.append(l)
    
    with open(os.path.expanduser("~/.bashrc"), 'w') as f:
        print("\n".join(new_data), file=f, end='')

if __name__ == '__main__':
    
    path = os.path.abspath(os.path.join(os.curdir, 'sapp.py'))
    cammand = f"alias sapp='python {path}'"
    remove_cammand(cammand)

    cammand = "alias spython='sapp python'"
    remove_cammand(cammand)

    if os.path.exists(os.path.expanduser("~/.sapp")):
        os.remove(os.path.expanduser("~/.sapp"))