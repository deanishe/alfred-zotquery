#!/usr/bin/env python
# encoding: utf-8
#
# Copyright (c) 2017 Dean Jackson <deanishe@deanishe.net>
#
# MIT Licence. See http://opensource.org/licenses/MIT
#
# Created on 2017-12-15
#

"""
"""

from __future__ import absolute_import

from HTMLParser import HTMLParser


class HTMLText(HTMLParser):
    """Extract text from HTML.

    Strips all tags from HTML.

    Attributes:
        data (list): Accumlated text content.

    """

    @classmethod
    def strip(cls, html, decode=True):
        """Extract text from HTML.

        Args:
            html (str): HTML to process.
            decode (bool, optional): Decode from UTF-8 to Unicode.

        Returns:
            basestring: `str` or `unicode` text content of HTML.
        """
        s = cls()
        s.feed(html)
        if decode:
            return unicode(s)
        return str(s)

    def __init__(self):
        self.reset()
        self.data = []

    def handle_data(self, s):
        self.data.append(s)

    def __str__(self):
        return ''.join(self.data)

    def __unicode__(self):
        return unicode(str(self), 'utf-8', errors='replace')
