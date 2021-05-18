#!/usr/bin/env bash
#SBATCH --array=1-16
#SBATCH --cpus-per-task=4
#SBATCH --nodes=1
#SBATCH --mem=8768M
#SBATCH --output=logs/mobi_%A_%a.log
#SBATCH --job-name=Mobile
#SBATCH --partition=1day

source ../../scripts/env.sh
exec jug execute
