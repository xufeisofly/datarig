from baselines.mappers.filters.content_filters import *


if __name__ == '__main__':
    data = """
Multicritical points and crossover mediating the strong violation of universality: Wang-Landau determinations in the random-bond d=2 Blume-Capel model

Malakis, A. and Berker, A. Nihat and Hadjiagapiou, I. A. and Fytas, N. G. and Papakonstantinou, T. (2010) Multicritical points and crossover mediating the strong violation of universality: Wang-Landau determinations in the random-bond d=2 Blume-Capel model.

Full text not available from this repository.

Official URL: http://arxiv.org/abs/1001.1240


The effects of bond randomness on the phase diagram and critical behavior of the square lattice ferromagnetic Blume-Capel model are discussed. The system is studied in both the pure and disordered versions by the same efficient two-stage Wang-Landau method for many values of the crystal field, restricted here in the second-order phase transition regime of the pure model. For the random-bond version several disorder strengths are considered. We present phase diagram points of both pure and random versions and for a particular disorder strength we locate the emergence of the enhancement of ferromagnetic order observed in an earlier study in the ex-first-order regime. The critical properties of the pure model are contrasted and compared to those of the random model. Accepting, for the weak random version, the assumption of the double logarithmic scenario for the specific heat we attempt to estimate the range of universality between the pure and random-bond models. The behavior of the strong disorder regime is also discussed and a rather complex and yet not fully understood behavior is observed. It is pointed out that this complexity is related to the ground-state structure of the random-bond version.

Item Type:Article
Subjects:Q Science > QC Physics
ID Code:13760
Deposited By:A. Nihat Berker
Deposited On:16 Feb 2010 09:13
Last Modified:16 Feb 2010 09:13

Repository Staff Only: item control page    
    """
    page = {
        'text': data,
    }
    ret = massive_web_repetition_filters(page, tokenizer='uniseg', annotate=True, token="", debug=True)
    print(ret)
