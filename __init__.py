# -*- coding: utf-8 -*-

from .views import ingestion
from .profiles import profiles
from .jobs import jobs
from .origin_video import origin_video
from ..extensions import mdb
from .models import Profile, OriginVideo, EncodingJob

mdb.register([Profile, OriginVideo, EncodingJob])
