#!/bin/bash
#
#SBATCH --job-name=nli_main
#SBATCH --partition=m40-short # Partition to submit to 
#
#SBATCH --ntasks=1
#SBATCH --time=240:00         # Runtime in D-HH:MM
#SBATCH --mem-per-cpu=11000    # Memory in MB per cpu allocated

sleep 120
echo I sleep and die
