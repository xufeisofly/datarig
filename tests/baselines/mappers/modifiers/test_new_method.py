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
    text = """Student Life Category

February 6, 2018
0

Education…at what cost?

With spring break a couple weeks from now, everyone is talking about snow days and two-hour delays. Yet, while sleeping…

February 6, 2018
0

What’s Happening in C4?

February 1, 2018
0

Student’s Choice

Junior Isabella Seavers has been on prom committee for two years. North, East, and CSA students come together to put…

January 26, 2018
0

Bringing Cheer Into Senior Projects

January 9, 2018, young cheerleaders preformed in the Memorial Gym at halftime of the North vs. Franklin Community High School…

January 25, 2018
0

Pancake Day for Dance Marathon

Be sure to support dance marathon in their pancake fundraiser tomorrow morning. For $1 you can get 3 pancakes and…

January 24, 2018
0

Formal Slide Show 2018

Pictures by Paola Fernandez.

January 23, 2018
0

Competition Season

For many North students passionate about the performing arts, the beginning of the year is an exciting time. Every new…

December 19, 2017
0

Merry Music for the Holidays

A multitude of Christmas music flows out of the auditorium as the band prepares for their annual winter concert. “I’m…

December 14, 2017
0

Facing Finals

Christmas season brings with it snow, gifts, joy, and finals. With break right around the corner, students are rushing to…

December 13, 2017
0

Driver Safety this Winter

Driving requires a lot more than understanding road signs and traffic laws, especially in the winter. As winter approaches student…"""
    page = {CONTENT: text}
    print(short_line_modifier(page)[0]['text'])
    
    # Test with an empty content
    empty_page = {CONTENT: ''}
    print(short_line_modifier(empty_page))
    
    
if __name__ == '__main__':
    test_short_line_modifier()