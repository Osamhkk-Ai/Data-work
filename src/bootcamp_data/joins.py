import pandas as pd 

# put left dataFrame and the right then on what the same column in the data frame and  chose the valdate  (one to one or many) whatever the suffixes
def safe_left_join(DFleft ,DFright ,On ,Validate ,Suffixes):
    return pd.merge(DFleft,DFright, how= "left", on=On, validate = Validate,suffixes=Suffixes) 