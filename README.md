# SAPP

`sapp` is a command helper for slurm system. It allows you to act as if you are on compute node (with Internet!). No need to learn how to use slurm.

## How to use

### Quick Start

Use `spython` to replace `python` and your applications will run on the compute node.

You will be asked to select a compute node for the first time. `sapp` will show the partition, type of cards and how many resources are available. After that, `sapp` will remember your choice and next time you may select this config with one click.

<div align=center>
    <img src="imgs/demo01.gif">
</div>

### Advanced Usage

If you want to execute commands other that `python`, just add `sapp` in front of your command. Actually, `spython` is just a shortcut for `sapp python`. For example, use `sapp bash train.sh` to run `bash train.sh` on the compute node.

<div align=center>
    <img src="imgs/demo02.gif">
</div>

By default, sapp will do port forwarding for you. You will be able to use the Internet on the compute node as if you are on your local machine.

### Slash

`sapp` now uses [slash](https://github.com/why-in-Shanghaitech/slash) to provide Internet service. You may select the preferred slash service when submitting the job. If you do not want to use slash, just select `none`.

```bash
# create an slash environment with a subscription link
# e.g. https://raw.githubusercontent.com/Pawdroid/Free-servers/main/sub
slash create -n myenv -f <your_subscription>

# submit a job, choosing slash=myenv
sapp wget huggingface.co
```

## Install

```sh
pip install git+https://github.com/why-in-Shanghaitech/sapp.git
```

## Uninstall

```sh
pip uninstall sapp
```

## Features

 - Free yourself from long commands and slurm settings. Personally, I do not like typing a long command or executing a shell script with no interactive console.
 - Beautiful `tqdm` progress bar for `srun` interactive jobs.
 - Sapp allows you to run `srun` and `sbatch` without worrying about file changes. It will memorize the file you submit, so feel free to change the scripts or config files after submitting the job, even if it does not start running yet.
 - Sapp could automatically setup the slash service -- enjoy the Internet on the compute node!

## How it works

 - The GPU status query is based on the command `sinfo`.
 - The Internet service is based on slash.


