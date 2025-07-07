import pytest
import warnings
import copy
from baselines.core.constants import CONTENT

from baselines.mappers.modifiers import bad_words_modifier,email_and_phone_removal_modifier, short_line_modifier

def test_prohibited_words_modifier():
    page = {CONTENT: "This is a test page with some prohibited words. like 'baby juice', 'tesbbwt', and 'ball licking'."} 
    print(bad_words_modifier(page))
    print(bad_words_modifier({CONTENT:''}))

def test_email_and_phone_removal_modifier():
    page = {CONTENT: "This is a test page with some like '1234@qq.vom', '+23-18933302213', and '800-820-8888'."} 
    print(email_and_phone_removal_modifier()(page))

def test_short_line_modifier():
    text = """"""
    page = {CONTENT: text}
    print(short_line_modifier(page)[0]['text'])
    
    # Test with an empty content
    empty_page = {CONTENT: ''}
    print(short_line_modifier(empty_page))
    
    
if __name__ == '__main__':
    test_short_line_modifier()