# -*- coding: utf-8 -*-
from fabric.operations import local

__author__ = 'patrickz'

def push():
    local('git add .; git commit -m "update"; git push origin master')