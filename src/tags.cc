/* Copyright (c) 2007-2009 Antony Dovgal <tony@daylessday.org>

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

#include "pinba.h"
#include "pinba_map.h"
#include "pinba_lmap.h"

void pinba_tag_dtor(pinba_tag *tag) /* {{{ */
{
	pinba_lmap_delete(D->tag.table, tag->id);
	pinba_map_delete(D->tag.name_index, tag->name);

	free(tag);
}
/* }}} */

pinba_tag *pinba_tag_get_by_name(char *name) /* {{{ */
{
	pinba_tag *tag;

	tag = (pinba_tag *)pinba_map_get(D->tag.name_index, name);
	if (UNLIKELY(!name)) {
		return NULL;
	}

	return tag;
}
/* }}} */

pinba_tag *pinba_tag_get_by_id(size_t id) /* {{{ */
{
	pinba_tag *tag;

	tag = (pinba_tag *)pinba_lmap_get(D->tag.table, id);
	if (UNLIKELY(!tag)) {
		return NULL;
	}

	return tag;
}
/* }}} */

/* 
 *
 * vim600: sw=4 ts=4 fdm=marker
 */
