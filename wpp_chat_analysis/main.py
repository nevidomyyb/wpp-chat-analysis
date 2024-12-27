from Raw import Raw
from Staged import Staged
from Curated import Curated

raw_layer = Raw('./raw/')
raw_layer.create('SLChat.txt')
staged_layer = Staged('./staged/')
staged_layer.create()
curated_layer = Curated('./curated/')
curated_layer.create()