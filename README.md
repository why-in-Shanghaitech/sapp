# SAPP

`sapp` is a command helper for slurm system. It allows you to act as if you are on compute node (with Internet!). No need to learn how to use slurm.

## How to use

Type `spython` to replace `python` and run your applications.

Use arrow keys to select, type to input.

<div align=center>
    <img src="imgs/demoPlay.gif">
</div>

If you want to execute commands other that `python`, just add `sapp` in front of your command. Actually, `spython` is just a short cut for `sapp python`.

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
 - Sapp could do auto port forwarding -- enjoy the Internet on the compute node!

## How it works

 - The GPU status query is based on the command `sinfo`.
 - The Internet service is based on clash and ssh port forwarding.

> **Warning**  
> This tool is only tested in Shanghaitech SIST Cluster. It may not suit for all slurm systems. (But easy to extend!)

