'''
This script classifies a given text as either 'CC' or 'Wikipedia' using a FastText model from the RedPajama project.
More details about the model can be found in this GitHub issue: https://github.com/togethercomputer/RedPajama-Data/issues/24
Model download is done via `setup.py` script.
The download link is: https://drive.google.com/file/d/1DnsfpWWE0jFPCoYe6clwqb3Ub5Ac92s1/view?usp=share_link
'''
import os
from typing import Dict, List, Callable

import fasttext
import ray

from core.constants import CONTENT
from core.factory_utils import factory_function


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
MODEL_SUBDIRECTORY = "baselines/mappers/enrichers/quality_prediction_enrichment_models"
RPJ_MODEL_FILENAME = 'model.bin'

def load_fasttext_model(model_filename):
    if os.path.exists(MODEL_SUBDIRECTORY):
        model_path = os.path.join(MODEL_SUBDIRECTORY, model_filename)
    else:
        model_path = os.path.join(PROJECT_ROOT, MODEL_SUBDIRECTORY, model_filename)

    assert os.path.exists(model_path), (
        f"Model {model_path} does not exist. "
        "Please download the model to this path before running a baselines pipeline involving fasttext filtering. "
        "See https://github.com/mlfoundations/dclm/blob/main/baselines/README.md#fasttext-filtering for more details."
    )

    return fasttext.load_model(model_path)


MODEL_HOLDER_REF = None

def load_shared_fasttext_model(model_filename):
    global MODEL_HOLDER_REF
    
    # 如果已经存在模型持有者引用，直接返回
    if MODEL_HOLDER_REF is not None:
        try:
            # 验证 Actor 是否仍然存活
            ray.get_actor("fasttext_model_holder")
            return MODEL_HOLDER_REF
        except ValueError:
            # Actor 不存在，重置引用
            MODEL_HOLDER_REF = None
    
    # 确定模型路径
    if os.path.exists(MODEL_SUBDIRECTORY):
        model_path = os.path.join(MODEL_SUBDIRECTORY, model_filename)
    else:
        model_path = os.path.join(PROJECT_ROOT, MODEL_SUBDIRECTORY, model_filename)

    # 验证文件存在
    assert os.path.exists(model_path), (
        f"Model {model_path} does not exist. "
        "Please download the model to this path before running a baselines pipeline involving fasttext filtering. "
        "See https://github.com/mlfoundations/dclm/blob/main/baselines/README.md#fasttext-filtering for more details."
    )

    # 定义模型持有者 Actor
    @ray.remote(num_cpus=0)  # 使用 0 CPU 资源，不占用计算资源
    class ModelHolder:
        def __init__(self, path):
            self.model = fasttext.load_model(path)
        
        def predict(self, text):
            return self.model.predict(text)
    
    # 尝试获取已存在的 Actor
    try:
        MODEL_HOLDER_REF = ray.get_actor("fasttext_model_holder")
    except ValueError:
        # 不存在则创建新的
        MODEL_HOLDER_REF = ModelHolder.options(
            name="fasttext_model_holder", 
            lifetime="detached"
        ).remote(model_path)
    
    return MODEL_HOLDER_REF


def classify_fasttext_hq_prob(model: fasttext.FastText._FastText, content: str, label_name=None) -> dict:
    '''
    This function classifies a given text as either 'CC' or 'Wikipedia' and returns the label along with its probability.

    Parameters:
    model (fasttext.FastText._FastText): The FastText model to use for the classification.
    content (str): The text to classify.

    Returns:
    dict: A value for 'hq_prob' - the probability to be a high-quality page.
    '''
    # Clean the input text by joining all lines into a single string
    text = " ".join(content.strip().splitlines())

    # Make the prediction
    pred = model.predict(text)
    # Extract the predicted label and its probability
    (pred_label, pred_prob) = pred

    # 如果用户指定了某个标签名称，尝试找到它的概率
    if label_name:
        for lab, p in zip(pred_label, pred_prob):
            if lab == label_name:
                return p


    pred_label = pred_label[0]
    hq_prob = pred_prob[0]        

    # If the predicted label is 'CC', adjust the probability of it being 'Wikipedia'
    if pred_label == "__label__cc":
        hq_prob = 1 - pred_prob

    # Return the output
    return hq_prob


def classify_fasttext_hq_prob_ray(model_holder, content: str, label_name=None) -> dict:
    # Clean the input text by joining all lines into a single string
    text = " ".join(content.strip().splitlines())


    # Make the prediction
    pred = ray.get(model_holder.predict.remote(text))

    # Extract the predicted label and its probability
    (pred_label, pred_prob) = pred

    # 如果用户指定了某个标签名称，尝试找到它的概率
    if label_name:
        for lab, p in zip(pred_label, pred_prob):
            if lab == label_name:
                return p

    pred_label = pred_label[0]
    hq_prob = pred_prob[0]    

    # If the predicted label is 'CC', adjust the probability of it being 'Wikipedia'
    if pred_label == "__label__cc":
        hq_prob = 1 - hq_prob

    # Return the output
    return hq_prob


@factory_function
def classify_fasttext_hq_prob_enricher(model_filename=RPJ_MODEL_FILENAME, key: str = "fasttext_hq_prob", overwrite: bool = False, label_name=None, **kwargs) -> Callable[
    [Dict], List[Dict]]:
    '''
    Enriches the given page with the text type (CC or Wikipedia).

    Parameters:
        page (dict): The page to enrich.
        model_filename (str): The name of the fasttext model file. Assumes it is placed in MODEL_SUBDIRECTORY.
        key (str): The key to store the text type under.
        overwrite (bool): Whether to overwrite the existing value of the key.

    Returns:
        A function that enriches the given page with the text type (HQ or CC).
    '''
    model_holder = load_shared_fasttext_model(model_filename)

    def enrich(page: Dict) -> List[Dict]:
        assert overwrite or key not in page, f"cannot overwrite an existing key {key}"
        page[key] = classify_fasttext_hq_prob_ray(model_holder, page[CONTENT], label_name=label_name)
        return [page]

    return enrich
