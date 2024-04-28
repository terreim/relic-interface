import customtkinter as ctk
import requests
import re
import pandas as pd
import time
import numpy as np
import matplotlib.pyplot as plt
from pymongo import MongoClient

client = MongoClient("localhost", 27017)
db = client["static_data"]  
prime_parts_collection = db["prime_parts_collection"]
relics_collection = db["relics_collection"]
relic_rewards = db["relic_rewards"]

ctk.set_appearance_mode("System")
ctk.set_default_color_theme("blue")

class LoadAPI:
    ...


    def __init__(self, app, sidebar, main_pages, events, reload):
        self.app = app
        self.sidebar = sidebar
        self.main_pages = main_pages
        self.events = events
        self.reload = reload

        self.csv_exist = False
        self.csv_status = False
        self.api_status = False

        
    def load_relic(self):
        ...
            
    def load_pages(self, item_name):
        ...
    
    def load_order(self, order):
        ...
    
    def load_events(self, info):
        ...

    

class App(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.title("Squad Relic UI")
        self.geometry(f"{1000}x{550}")
        
        self.grid_columnconfigure(1, weight=1)
        self.grid_columnconfigure((0, 2), weight=0)
        self.grid_rowconfigure((0, 1), weight=1)
        self.grid_rowconfigure((2, 3), weight=0)

class SideBar(App):
    def __init__(self, master):
        self.frm_sidebar = ctk.CTkFrame(master, width=200, corner_radius=5)
        self.frm_sidebar.grid(row=0, column=0, rowspan=4, padx=(15, 0), pady=(10, 10), sticky="nsew")

        #Frame - Sidebar - Relic types
        self.option_relic_types = ctk.CTkComboBox(self.frm_sidebar, corner_radius=5,
                                                            values=["All", "Lith", "Meso", "Neo", "Axi"])
        self.option_relic_types.grid(row=0, column=0, sticky="nsew")
        self.option_relic_types.set("All")
        
        #Frame - Sidebar - Relic choice
        relic_choice_var = ctk.StringVar(value="Name")
        self.option_relic_choice = ctk.CTkComboBox(self.frm_sidebar, corner_radius=5,
                                                            values=["Name", "Platinum Profit", "Ducat Profit"], variable=relic_choice_var)
        self.option_relic_choice.grid(row=0, column=1, sticky="nsew")
        self.option_relic_choice.set("Name")
        
        #Frame - Sidebar - Load relic test
        self.frm_relic_list = ctk.CTkScrollableFrame(self.frm_sidebar, corner_radius=5)
        self.frm_relic_list.grid(row=1, column=0, columnspan=2, sticky="nsew")
        LoadAPI.load_relic(self.frm_relic_list)


class MainPages(App):
    def __init__(self, master):
        self.pageview = ctk.CTkTabview(master, width=500)
        self.pageview.grid(row=0, column=1, sticky="nsew", padx=(20, 10))
        self.pageview.add("Relic")
        self.pageview.add("Simulations")
        self.pageview.add("Market")
        self.pageview.add("Settings")
        self.pageview._segmented_button.grid(sticky="ew")


class Events(App):
    def __init__(self, master):
        self.frm_events = ctk.CTkFrame(master, width=200, corner_radius=5)
        self.frm_events.grid(row=0, column=2, padx=(10, 15), pady=(15, 0), sticky="nsew")
        self.frm_events.grid_columnconfigure(0, weight=0)
        self.frm_events.grid_rowconfigure((0, 1, 2), weight=1)

        #Frame - Events - Current unvaulted relic
        self.frm_unvalted_relic = ctk.CTkFrame(self.frm_events, corner_radius=5)
        self.frm_unvalted_relic.grid(row=0, column=0, padx=(10, 10), pady=(10, 10), sticky="nsew")
        
        #Frame - Events - Current fissure
        self.frm_relic_fiss = ctk.CTkFrame(self.frm_events, corner_radius=5)
        self.frm_relic_fiss.grid(row=1, column=0, padx=(10, 10), pady=(10, 10), sticky="nsew")

        #Frame - Events - Sellers
        self.frm_relic_seller = ctk.CTkScrollableFrame(self.frm_events, corner_radius=5)
        self.frm_relic_seller.grid(row=2, column=0, padx=(10, 10), pady=(10, 10), sticky="nsew")

class SearchBar(App):
    def __init__(self, master):
        self.frm_search_bar = ctk.CTkEntry(master, placeholder_text="Relic Name or Prime Part", corner_radius=5)
        self.frm_search_bar.grid(row=2, column=1, padx=(20, 10), pady=(10, 0), sticky="nsew")

class DeleteCell(App):
    def __init__(self, master):
        self.btn_clear_cell = ctk.CTkButton(master, text="Clear Relic", fg_color="transparent", border_width=2, text_color=("gray10", "#DCE4EE"))
        self.btn_clear_cell.grid(row=2, column=2, padx=(10, 15), pady=(10, 0), sticky="nsew")

class ProgressBar(App):
    def __init__(self, master):
        self.bar_reload_bar = ctk.CTkProgressBar(master, height=26, corner_radius=5)
        self.bar_reload_bar.grid(row=3, column=1, padx=(20, 10), sticky="ew")

class ReloadAPI(App):
    def __init__(self, master):
        self.btn_reload_api = ctk.CTkButton(master, text="Reload App", fg_color="transparent", border_width=2, text_color=("gray10", "#DCE4EE"))
        self.btn_reload_api.grid(row=3, column=2, padx=(10, 15), pady=(10, 10), sticky="nsew")



if __name__ == "__main__":
    app = App()
    sidebar = SideBar(app)
    main_pages = MainPages(app)
    events = Events(app)
    search_bar = SearchBar(app)
    delete_cell = DeleteCell(app)
    progress_bar = ProgressBar(app)
    reload = ReloadAPI(app)
    a_api = LoadAPI(app, sidebar, main_pages, events, reload)

    app.mainloop()