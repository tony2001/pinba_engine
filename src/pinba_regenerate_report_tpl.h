
static inline pinba_report *PINBA_REGENERATE_REPORT_FUNC_D()/* pinba_regenerate_report9(PINBA_SHARE *share) */ /* {{{ */
{
	PPvoid_t ppvalue;
	pinba_report *report;
	pinba_pool *request_pool = &D->request_pool;
	pinba_stats_record *record;
	unsigned int i;
	PINBA_REPORT_DATA_STRUCT_D();
	/*struct pinba_report9_data *data;*/
	PINBA_REPORT_INDEX_D();
	/*uint8_t index[PINBA_STATUS_SIZE + 1 + PINBA_SCRIPT_NAME_SIZE] = {0};*/

	ppvalue = JudySLGet(D->base_reports, share->index, NULL);
	if (!ppvalue) {

		report = (pinba_report *)calloc(1, sizeof(pinba_report));
		if (!report) {
			return NULL;
		}

		pinba_parse_conditions(share, (pinba_std_report *)report);

		report->std.index = (uint8_t *)strdup((const char *)share->index);
		report->std.type = PINBA_REPORT_ID()/*PINBA_TABLE_REPORT9*/;
		report->std.time_interval = 1;
		report->add_func = PINBA_UPDATE_REPORT_ADD_FUNC()/*pinba_update_report9_add*/;
		report->delete_func = PINBA_UPDATE_REPORT_DELETE_FUNC()/*pinba_update_report9_delete*/;
		pthread_rwlock_init(&report->std.lock, 0);
		pthread_rwlock_wrlock(&report->std.lock);

		ppvalue = JudySLIns(&D->base_reports, share->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			pthread_rwlock_unlock(&report->std.lock);
			pthread_rwlock_destroy(&report->std.lock);
			pinba_std_report_dtor(report);
			free(report);
			return NULL;
		}

		if (pinba_base_reports_array_add(report) < 0) {
			JudySLDel(&D->base_reports, share->index, NULL);
			pthread_rwlock_unlock(&report->std.lock);
			pthread_rwlock_destroy(&report->std.lock);
			free(report);
			return NULL;
		}
		*ppvalue = report;

		pthread_mutex_lock(&pinba_mutex);
		ppvalue = JudySLIns(&D->tables_to_reports, share->index, NULL);
		if (ppvalue) {
			*ppvalue = report;
		}
		pthread_mutex_unlock(&pinba_mutex);

	} else {
		report = (pinba_report *)*ppvalue;
		return report;
	}

	pthread_rwlock_unlock(&report->std.lock);
	return report;
}
/* }}} */

