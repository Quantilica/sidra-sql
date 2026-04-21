# Copyright (C) 2026 Komesu, D.K. <daniel@dkko.me>
#
# This file is part of sidra-sql.
#
# sidra-sql is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# sidra-sql is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with sidra-sql.  If not, see <https://www.gnu.org/licenses/>.

import logging

from . import config, database, sidra, storage

__all__ = ["config", "database", "sidra", "storage"]

logging.getLogger(__name__).addHandler(logging.NullHandler())

config.setup_logging(__name__, "sidra-sql.log")
