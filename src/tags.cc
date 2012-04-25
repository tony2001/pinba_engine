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

void pinba_tag_dtor(pinba_tag *tag) /* {{{ */
{
	JudyLDel(&D->tag.table, tag->id, NULL);
	JudySLDel(&D->tag.name_index, (uint8_t *)tag->name, NULL);

	free(tag);
}
/* }}} */

pinba_tag *pinba_tag_get_by_name(const unsigned char *name) /* {{{ */
{
	pinba_tag *tag;
	PPvoid_t ppvalue;

	ppvalue = JudySLGet(D->tag.name_index, (uint8_t *)name, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		return NULL;
	}

	tag = (pinba_tag *)*ppvalue;
	return tag;
}
/* }}} */

pinba_tag *pinba_tag_get_by_name_next(unsigned char *name) /* {{{ */
{
	pinba_tag *tag;
	PPvoid_t ppvalue;

	ppvalue = JudySLNext(D->tag.name_index, (uint8_t *)name, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		return NULL;
	}

	tag = (pinba_tag *)*ppvalue;
	return tag;
}
/* }}} */

pinba_tag *pinba_tag_get_by_id(size_t id) /* {{{ */
{
	pinba_tag *tag;
	PPvoid_t ppvalue;

	ppvalue = JudyLGet(D->tag.table, (Word_t)id, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		return NULL;
	}

	tag = (pinba_tag *)*ppvalue;
	return tag;
}
/* }}} */

void pinba_tag_delete_by_name(const unsigned char *name) /* {{{ */
{
	pinba_tag *tag;
	PPvoid_t ppvalue;

	ppvalue = JudySLGet(D->tag.name_index, (uint8_t *)name, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		return;
	}

	tag = (pinba_tag *)*ppvalue;
	pinba_tag_dtor(tag);
}
/* }}} */

/* 
 *
 * vim600: sw=4 ts=4 fdm=marker
 */
