db_cols = [
    # Juniors
    {'name': 'AT',      'action': 'MIN',    'db_col': 'none'},
    {'name': 'AT',      'action': 'AVE',    'db_col': 'airT'},
    {'name': 'AT',      'action': 'MAX',    'db_col': 'none'},
    {'name': 'AT_AWS',  'action': 'AVE',    'db_col': 'airT'},
    {'name': 'AT_Cnpy', 'action': 'AVE',    'db_col': 'canT'},

    {'name': 'RH',      'action': 'AVE',    'db_col': 'rh'},
    {'name': 'RH_AWS',  'action': 'AVE',    'db_col': 'rh'},
    {'name': 'RH_Cnpy', 'action': 'AVE',    'db_col': 'canRH'},

    {'name': 'sin',     'action': 'AVE',    'db_col': 'none'},
    {'name': 'cos',     'action': 'AVE',    'db_col': 'none'},
    {'name': 'Vref',    'action': 'AVE',    'db_col': 'none'},

    {'name': 'LeafWet', 'action': 'AVE',    'db_col': 'leaf'},
    {'name': 'GSR',     'action': 'AVE',    'db_col': 'gsr'},

    {'name': 'h2',      'action': 'MIN',    'db_col': 'none'},
    {'name': 'h2',      'action': 'AVE',    'db_col': 'batt'},
    {'name': 'h2',      'action': 'MAX',    'db_col': 'none'},
    {'name': 'a7',      'action': 'AVE',    'db_col': 'batt'},
    {'name': 'h7',      'action': 'AVE',    'db_col': 'batt'},
    {'name': 'h15',      'action': 'AVE',    'db_col': 'none'},
    {'name': 'Batt',    'action': 'MIN',    'db_col': 'none'},
    {'name': 'Batt',    'action': 'AVE',    'db_col': 'batt'},
    {'name': 'Batt',    'action': 'MAX',    'db_col': 'none'},

    {'name': 'RN',      'action': 'TOT2',   'db_col': 'rain'},
    {'name': 'RN',      'action': 'TOT1',   'db_col': 'rain'},
    {'name': 'C0',      'action': 'TOT1',    'db_col': 'rain'},

    {'name': 'ST',      'action': 'AVE',    'db_col': 'soilT'},
    {'name': 'SoilTmp', 'action': 'AVE',    'db_col': 'soilT'},
    {'name': 'SoilTemp','action': 'AVE',    'db_col': 'soilT'},
    {'name': 'SDIID0',  'action': 'AVE',     'db_col': 'soilT'},

    {'name': 'WndSpd',  'action': 'MIN',    'db_col': 'Wmin'},
    {'name': 'WndSpd',  'action': 'AVE',    'db_col': 'Wavg'},
    {'name': 'WndSpd',  'action': 'MAX',    'db_col': 'Wmax'},
    {'name': 'WS',      'action': 'MIN',    'db_col': 'Wmin'},
    {'name': 'WS',      'action': 'AVE',    'db_col': 'Wavg'},
    {'name': 'WS',      'action': 'MAX',    'db_col': 'Wmax'},

    {'name': 'h5',      'action': 'AVE',    'db_col': 'pressure'},
    {'name': 'BP',      'action': 'AVE',    'db_col': 'pressure'},

    {'name': 'PanCurrentLevel',      'action': 'AVE',    'db_col': 'none'},
    {'name': 'Blank',      'action': 'AVE',    'db_col': 'none'},
    {'name': 'Dewpoint',   'action': 'AVE',    'db_col': 'none'}

]