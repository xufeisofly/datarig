import pytest
import warnings
import copy
from baselines.core.constants import CONTENT

from baselines.mappers.modifiers import prohibited_words_modifier,email_and_phone_removal_modifier

def test_prohibited_words_modifier():
    page = {CONTENT: "This is a test page with some prohibited words. like 'baby juice', 'tesbbwt', and 'ball licking'."} 
    print(prohibited_words_modifier()(page))

def test_email_and_phone_removal_modifier():
    page = {CONTENT: "This is a test page with some like '1234@qq.vom', '+23-18933302213', and '800-820-8888'."} 
    print(email_and_phone_removal_modifier()(page))

    
if __name__ == '__main__':
    test_email_and_phone_removal_modifier()
    