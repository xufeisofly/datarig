from baselines.mappers.filters.content_filters import *


if __name__ == '__main__':
    data = """
Ergodicity of robust switching control and nonlinear system of quasi variational inequalities

Abstract : We analyze the asymptotic behavior for a system of fully nonlinear parabolic and elliptic quasi variational inequalities. These equations are related to robust switching control problems introduced in [3]. We prove that, as time horizon goes to infinity (resp. discount factor goes to zero) the long run average solution to the parabolic system (resp. the limiting discounted solution to the elliptic system) is characterized by a solution of a nonlinear system of ergodic variational inequalities. Our results hold under a dissipativity condition and without any non degeneracy assumption on the diffusion term. Our approach uses mainly probabilistic arguments and in particular a dual randomized game representation for the solution to the system of variational inequalities.
Type de document :
Pré-publication, Document de travail
to appear in SIAM Journal on Control and Optimization. 2017
Liste complète des métadonnées
Contributeur : Huyen Pham <>
Soumis le : vendredi 3 février 2017 - 22:13:54
Dernière modification le : vendredi 17 février 2017 - 16:11:53


Fichiers produits par l'(les) auteur(s)


  • HAL Id : hal-01104773, version 2
  • ARXIV : 1501.04477


Erhan Bayraktar, Andrea Cosso, Huyên Pham. Ergodicity of robust switching control and nonlinear system of quasi variational inequalities . to appear in SIAM Journal on Control and Optimization. 2017. <hal-01104773v2>



Consultations de
la notice


Téléchargements du document    
    """
    page = {
        'text': data,
    }
    ret = massive_web_repetition_filters(page, tokenizer='fasttext', annotate=True, token="", debug=True)
    print(ret)
