import pytest
import warnings
import copy
from baselines.core.constants import CONTENT,TERMINAL_PUNCTUATION

from baselines.mappers.modifiers import bad_words_modifier,email_and_phone_removal_modifier, short_line_modifier
from baselines.mappers.filters.content_filters import line_punct_ratio_filter

def test_prohibited_words_modifier():
    page = {CONTENT: "This is a test page with some prohibited words. like 'baby juice', 'tesbbwt', and 'ball licking'."} 
    print(bad_words_modifier(page))
    print(bad_words_modifier({CONTENT:''}))

def test_email_and_phone_removal_modifier():
    page = {CONTENT: "This is a test page with some like '1234@qq.vom', '+23-18933302213', and '800-820-8888'."} 
    print(email_and_phone_removal_modifier()(page))

def test_short_line_modifier():
    text = """Kendra Morris – Tickets – The Love Song – Los Angeles, CA – May 6th, 2018 | Spaceland Presents
The Regent, DTLA
The Love Song Bar
Pico Union Project
The Ritz San Jose
First Fridays at NHM
Santa Monica Pier
Levitt Los Angeles
Echo Park Rising
Sun May 6, 2018
The Love Song
Los Angeles, CA
This event is 21 and over
Sync event to iCal
Sync event to Google Calendar
Kendra Morris recalls singers who straddled soul and rock during the early '70s, such as Ruth Copeland and Chaka Khan. Her taste was developed through her parents' record collection, a library heavy on late-60s and early-70s soul and funk. During her childhood Morris's best friend was her karaoke machine, which she used for the sake of recording and learning to harmonize with herself. She eventually taught herself guitar and involved herself with a crappy band which moved to New York in 2004 and broke up soon after. Kendra stuck around and wound up recording songs by herself in the closet of her bushwick loft with an old Tascam 8 track she found at a shop in the east village and began releasing the demos on myspace and through iTunes. It was through these demos and playing shows around the Lower East Side of Manhattan with her Fender Mustang and a Sharp GF777 Boombox that she rigged her guitar to play through that she met her still constant producer and collaborator Jeremy Page. After touring with legendary Motown Funk Brother Dennis Coffey and releasing a series of singles including "Concrete Waves," with b-side remixed by DJ Premier, Morris released the album Banshee (2012) on Wax Poetics Records. In 2013, Morris returned with the covers album Mockingbird.
Both Banshee and Mockingbird received much acclaim, a cult following and several film + tv placements. The title track "Banshee" was used prominently in the Showtime hit series Ray Donovan and would end up as one of popular music seek app ‘Shazaams’ most searched for songs following the airing of the episode while her cover of Pink Floyd's "Shine On You Crazy Diamond" found itself in the trailer for major motion picture Dead Man Down starring Noomi Rapace and Colin Farrell.
Between international touring and promo for Banshee throughout 2013 and 2014, Morris worked on a side project band with friends Scarlett Johansson, Julia Haltigan and Holly Miranda. With Este Haim joining on drums, they released one single "Candy" and a cover of “Bizarre Love Triangle” for AMFARs The Time Is Now compilation record.
Returning with her own follow up EP titled Babble in 2016 to positive reviews and more placements with Black & Mild, the OWN Networks TV show Queen Sugar and then some she continued working on new tracks with Jeremy Page as well as added focus to her love of visual art and connecting it to her sonic world. Having been collaging and doing stop motion animation a handful of years for her own album art and videos she wound up in the directors chair as well as animating the 2018 music video “Bomb Thrown” for the hip hop supergroup Czarface and MF DOOM record Czarface Meets Metal Face. Popular online magazine Mass Appeal cited the video as one of the best of 2018.
February of 2018 Morris began releasing her own new music with her first single “Nothing” off of an upcoming record and the second single “Playing Games” following close behind in April along with a Greg Nice of Nice & Smooth on the Break Up Mix and her cover “Virgin” with DaM-FunK playing shoulder synth on the breakdown. With a vocal feature on track #8 Phantoms from the new Czarface/ MF DOOM album, more visual projects and music collaborations to come and a new record of her own on the way, Kendra seems to be both a creative reckoning in a league of her own.
There are currently no videos. Check back soon.
More Upcoming Events
Grand Ole Echo - Rob Leines
Maesa, Kyle McNeill, Nicholas Mudd Band, Ashleigh Flynn & the Riveters
Sun April 21
Tickets at the Door
Soft Rock Sunday
DJ Mara Schwartz Kuge
Sun April 21
The Love Song
M. Lockwood Porter Record Release with Katie Day
Sun April 21
The Love Song
2019 ? Spaceland Presents"""
    page = {CONTENT: text}
    lines = page[CONTENT].split("\n")
    lines = [line for line in lines if line.strip() != ""]
    
    stop_chars = tuple(TERMINAL_PUNCTUATION)
        
    ratio = sum(1 for line in lines if line.endswith(stop_chars)) / len(lines)
    print(f"Line punctuation ratio: {ratio}")
    # print(line_punct_ratio_filter(page))
    
    # Test with an empty content
    # empty_page = {CONTENT: ''}
    # print(short_line_modifier(empty_page))
    
    
if __name__ == '__main__':
    test_short_line_modifier()