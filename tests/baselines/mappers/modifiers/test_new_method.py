import pytest
import warnings
import copy
from baselines.core.constants import CONTENT,TERMINAL_PUNCTUATION,set_filter_reason_if_annotate

from baselines.mappers.modifiers import bad_words_modifier,email_and_phone_removal_modifier, short_line_modifier
from baselines.mappers.filters.content_filters import line_punct_ratio_filter, fineweb_quality_filter

def test_prohibited_words_modifier():
    page = {CONTENT: "This is a test page with some prohibited words. like 'baby juice', 'tesbbwt', and 'ball licking'."} 
    print(bad_words_modifier(page))
    print(bad_words_modifier({CONTENT:''}))

def test_email_and_phone_removal_modifier():
    page = {CONTENT: "This is a test page with some like '1234@qq.vom', '+23-18933302213', and '800-820-8888'."} 
    print(email_and_phone_removal_modifier()(page))

def test_short_line_modifier():
    text = """Tony Dungy puts Tom Brady into historical perspective -
FIND A STATION
THE DP APP
OFF THE SETON PATH
Find a Station
Friday: The new ManCave is open
Tony Dungy puts Tom Brady into historical perspective
February 1, 2019 Alan Guzzi	— No Comments
Topics: NFL, Patriots, Super Bowl, Tom Brady | Guests: Tony Dungy
Take Our Poll
Andrew Perloff @andrewperloffBoth @AlbertBreer and I think Mike Mayock would be happy taking @DevinWhite__40 at No. 4 for Raiders on @theMMQB TV… mins ago
Paul Pabst @PaulPabst@1013TheGame @dpshow @Twiddlemusic Fun day. See you soon. Heading to Burlington this summer.11 mins ago
Paul Pabst @PaulPabstTwiddle playing our post show cookout. mins ago
Dan Patrick Show @dpshowROCK AND ROLL!! mins ago
Seton @HiMyNameIsSetonROCK AND ROLL! mins ago
SweepTheLeg@dpshow Sorry but a caller brought it up. Thelma & Louise 'Women Love That Shit..." movie scene 1991 via @YouTube4 mins ago
Ron Arana@andrewperloff @dpshow @BarstoolBigCat How nice is this court!4 mins ago
Brandt BernatRT @dpshow: New Studio. mins ago
Chris W@dpshow when will the show be ready to go on @brlive ? Want to check out that new man cave.6 mins ago
Jeff@dpshow All this man cave needs is personal bed rooms when you all need a place to stay when in the doghouse or too drunk to drive home !6 mins ago
? 2019 The Dan Patrick Show | All rights reserved.
Advertise On The Dan Patrick Show"""
    page = {CONTENT: text}
    lines = page[CONTENT].split("\n")
    lines = [line for line in lines if line.strip() != ""]
    short_line_char_len = sum(len(line) for line in lines if len(line) <= 30)
    page_length = len(page[CONTENT].replace("\n", ""))
    print(short_line_char_len/page_length )
    stop_chars = tuple(TERMINAL_PUNCTUATION)
    
    ratio = sum(1 for line in lines if line.endswith(stop_chars)) / len(lines)
    print(ratio)
    # if ratio < 0.12 and not (ratio == 0 and False):
    #     if short_line_char_len/page_length > 0.1:
    #         print(set_filter_reason_if_annotate(page, "line_punct_ratio_filter", False))
    # print(fineweb_quality_filter(page))
    # Test with an empty content
    # empty_page = {CONTENT: ''}
    # print(short_line_modifier(empty_page))
    
    
if __name__ == '__main__':
    test_short_line_modifier()