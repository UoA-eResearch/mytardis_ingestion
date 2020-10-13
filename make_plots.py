# make_plots.py
#
# Functions to generate test plot from raw/processed thesis data files
#
# written by Chris Seal <c.seal@auckland.ac.nz>
#
# Last updated: 06 Aug 2020
#

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

sns.set_style("whitegrid")


def plot_tensile(filename,
                 plotfile):
    data = pd.read_csv(filename, sep='\t', names=['strain', 'stress'])
    fig, ax = plt.subplots()
    ax.set_xlabel(r'Strain $\frac{mm}{mm}$')
    ax.set_ylabel(r'Stress MPa')
    ax.plot(data['strain'],
            data['stress'],
            linestyle='-',
            marker='',
            color='#005824')
    fig.savefig(plotfile,
                dpi=300,
                orientation='landscape',
                format='png')
    return plotfile
