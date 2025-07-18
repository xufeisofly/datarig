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
    text = """Art UK | Discover Artworks
# [Skip to content] [Skip to main navigation] [Skip to quick links] [Go to accessibility information]
# How to donate
# Supporters and Partners
# Benefactors and donors
# Museums & Collections
# Remember me (uncheck on a public computer)
# Enter your email address below and we’ll send you a link to reset your password
# My details can be shared with selected Art UK Partners
# Sign up to the Art UK newsletter
# Murnau-Staffelsee I (detail), 1908, oil on paper laid on board by Wassily Kandinsky (1866–1944), photo credit: The Ashmolean Museum of Art and Archaeology
# Where artworks are held
# East Midlands of England
# East of England
# North West London
# South East London
# South West London
# North East England
# Tyne and Wear
# North West England
# South East England
# Isle of Wight
# South West England
# Isles of Scilly
# Yorkshire and the Humber
# East Riding of Yorkshire
# Isle of Man
# Central, Glasgow & Lothians
# Highlands and Islands
# Argyll and Bute
# Mid-Scotland and Fife
# Perth and Kinross
# North East Scotland
# Dumfries and Galloway
# West of Scotland
# North East Wales
# Denbighshire (Sir Ddinbych)
# Flintshire (Sir y Fflint)
# North West Wales
# Isle of Anglesey (Ynys Mon)
# South East Wales
# Bridgend (Pen-y-bont ar Ogwr)
# Merthyr Tydfil (Merthyr Tudful)
# Monmouthshire (Sir Fynwy)
# Neath Port Talbot (Castell-nedd Port Talbot)
# Rhondda Cynon Taf
# Vale of Glamorgan (Bro Morgannwg)
# South West Wales
# Carmarthenshire (Sir Gaerfyrddin)
# Pembrokeshire (Sir Benfro)
# Drawing & watercolour
# Mixed media & collage
# Architectural model or plan
# Mural or fresco
# Coat of arms
# Sign or marker
# Tomb or mausoleum
# Water fountain, trough or pump
# Animals and plants
# Fruit and vegetables
# Plants and flowers
# Trees and shrubs
# Arts and entertainment
# Architecture (the profession)
# Circuses, fairs and street entertainers
# Film and photography
# Reading and writing
# Fashion and costume
# Drapery and classical costume
# Dressmaking, patterns and materials
# Evening and formal costume
# Foreign and national costume
# Hairstyles, cosmetics and body art
# Jewellery, hats and accessories
# Military, religious and official costume
# Theatrical and fancy costume
# Underwear and nightwear
# Home and family
# Bathing and toilet
# Birth, marriage and death
# Eating and drinking (home and family)
# Furniture and interiors
# Housework and gardening
# Sickness and health
# Ideas and emotions
# Anger and hate
# Fear and horror
# Greed and gluttony
# Happiness and joy
# Life and death
# Love and desire
# Sadness and grief
# Sex and relationships
# Virtues and vices
# Literature and fantasy
# Dreams and nightmares
# Myths and fables
# Proverbs and sayings
# Hills and mountains
# Rivers and lakes
# Seas and coasts
# Times of day
# Nudes and models
# Power and politics
# Law and order
# Monarchy and aristocracy
# Parliaments and councils
# Social problems and reforms
# Religion and belief
# Angels and demons
# Ceremonies and rituals
# Magic and the occult
# Objects and symbols
# Places of worship
# Saints and martyrs
# Stories and people
# Worshippers and congregations
# Science and knowledge
# Invention and experiments
# Philosophy and thought
# Sport and leisure
# Eating and drinking (sport and leisure)
# Hobbies and pastimes
# Holidays and travel
# Hunting and fishing
# Toys and games
# Towns and buildings
# Bridges and viaducts
# Gardens and green spaces
# Ports and waterways
# Road and rail (towns and buildings)
# Stately homes and palaces
# Streets and squares
# Transport and industry
# Energy and fuel
# Machinery and tools
# Road and rail (transport and industry)
# Ships and boats
# War and conflict
# Aircraft (war and conflict)
# Armour and uniform
# Buildings and fortifications
# Servicemen and women
# Vehicles (war and conflict)
# Weapons and equipment
# Work and business
# Farming and fishing
# Shops and markets
# Trade and commerce
# Public Domain Mark and Public Domain Dedication (PD and CC0)
# Attribution (CC BY)
# Attribution-ShareAlike (CC BY-SA)
# Attribution-NoDerivs (CC BY-ND)
# Attribution-NonCommercial (CC BY-NC)
# Attribution-NonCommercial-ShareAlike (CC BY-NC-SA)
# Attribution-Non-Commercial-NoDerivs (CC BY-NC-ND)
# What do Creative Commons licences allow?
# Visit the Art UK Shop to licence high resolution images
# Artist: Mulard, Fran?ois Henri, 1769–1850	Remove
# Enter a name
# Start new search
# Fran?ois Henri Mulard
# Showing 1 artwork
# Sort by: Relevance
# Date made: new to old
# Date made: old to new
# Title: Z to A
# Artist: Z to A
# Portrait of a Gentleman	Fran?ois Henri Mulard (1769–1850)
# Portrait of a Gentleman	York Museums Trust
# Donate to Art UK
# ? is a registered trade mark of the Public Catalogue Foundation.
# Art UK is the operating name of the Public Catalogue Foundation, a charity registered in England and Wales () and Scotland (SC048601).
# New artworks, stories and chances to win prizes, delivered straight to your inbox every two weeks."""
    page = {CONTENT: text}
    # lines = page[CONTENT].split("\n")
    # lines = [line for line in lines if line.strip() != ""]
    # short_line_char_len = sum(len(line) for line in lines if len(line) <= 30)
    # page_length = len(page[CONTENT].replace("\n", ""))
    # # print(short_line_char_len/page_length )
    # stop_chars = tuple(TERMINAL_PUNCTUATION)
    # # print(ratio)17077
    # ratio = sum(1 for line in lines if line.endswith(stop_chars)) / len(lines)
    # if ratio < 0.12 and not (ratio == 0 and False):
    #     if short_line_char_len/page_length > 0.1:
    #         print(set_filter_reason_if_annotate(page, "line_punct_ratio_filter", False))
    print(fineweb_quality_filter(page))
    # Test with an empty content
    # empty_page = {CONTENT: ''}
    # print(short_line_modifier(empty_page))
    
    
if __name__ == '__main__':
    test_short_line_modifier()