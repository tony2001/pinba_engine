
static inline pinba_report *PINBA_REGENERATE_REPORT_FUNC_D()/* pinba_regenerate_report9(PINBA_SHARE *share) */ /* {{{ */
{
	PPvoid_t ppvalue;
	pinba_report *report;
	pinba_pool *p = &D->request_pool;
	pinba_stats_record *record;
	unsigned int i;
	PINBA_REPORT_DATA_STRUCT_D();
	/*struct pinba_report9_data *data;*/
	PINBA_REPORT_INDEX_D();
	/*uint8_t index[PINBA_STATUS_SIZE + 1 + PINBA_SCRIPT_NAME_SIZE] = {0};*/

	if (share->index[0] == '\0') {
		pinba_get_report_id(share);
	}

	ppvalue = JudySLGet(D->base_reports, share->index, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		report = (pinba_report *)calloc(1, sizeof(pinba_report));
		if (!report) {
			return NULL;
		}

		pinba_parse_conditions(share, (pinba_std_report *)report);

		report->std.type = PINBA_REPORT_ID()/*PINBA_TABLE_REPORT9*/;
		report->add_func = PINBA_UPDATE_REPORT_ADD_FUNC()/*pinba_update_report9_add*/;
		report->delete_func = PINBA_UPDATE_REPORT_DELETE_FUNC()/*pinba_update_report9_delete*/;
		pthread_rwlock_init(&report->lock, 0);
		pthread_rwlock_wrlock(&report->lock);

		ppvalue = JudySLIns(&D->base_reports, share->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			pthread_rwlock_unlock(&report->lock);
			pthread_rwlock_destroy(&report->lock);
			pinba_std_report_dtor(report);
			free(report);
			return NULL;
		}

		if (pinba_base_reports_array_add(report) < 0) {
			JudySLDel(&D->base_reports, share->index, NULL);
			pthread_rwlock_unlock(&report->lock);
			pthread_rwlock_destroy(&report->lock);
			free(report);
			return NULL;
		}
		*ppvalue = report;
	} else {
		report = (pinba_report *)*ppvalue;
		pthread_rwlock_wrlock(&report->lock);
	}

	pool_traverse_forward(i, p) {
		record = REQ_POOL(p) + i;

		CHECK_REPORT_CONDITIONS_CONTINUE(report, record);

		timeradd(&report->time_total, &record->data.req_time, &report->time_total);
		timeradd(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
		timeradd(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
		report->kbytes_total += record->data.doc_size;
		report->memory_footprint += record->data.memory_footprint;
		
#if PINBA_REPORT_NO_INDEX()
		report->results_cnt++;
		PINBA_UPDATE_HISTOGRAM_ADD(report, report->std.histogram_data, record->data.req_time);
#else
		{
			PINBA_INDEX_VARS_D();
			/*int index_len, dummy;*/

			PINBA_CREATE_INDEX_VALUE();
			/*
			   index_len = sprintf((char *)index, "%u:", record->data.status);
			   memcat_static(index, index_len, record->data.script_name, record->data.script_name_len, index_len);
			   */
			ppvalue = JudySLGet(report->results, index, NULL);

			if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
				/* no such value, insert */
				ppvalue = JudySLIns(&report->results, index, NULL);
				if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
					pthread_rwlock_unlock(&report->lock);
					return NULL;
				}
				data = (PINBA_REPORT_DATA_STRUCT() /*struct pinba_report9_data*/ *)calloc(1, sizeof(PINBA_REPORT_DATA_STRUCT()/*struct pinba_report9_data*/));

				PINBA_REPORT_ASSIGN_DATA();
				/*
				   memcpy_static(data->script_name, record->data.script_name, record->data.script_name_len, dummy);
				   data->status = record->data.status;
				   */
				*ppvalue = data;
				report->results_cnt++;
			} else {
				data = (PINBA_REPORT_DATA_STRUCT() /*struct pinba_report9_data*/ *)*ppvalue;
			}
			data->req_count++;
			timeradd(&data->req_time_total, &record->data.req_time, &data->req_time_total);
			timeradd(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
			timeradd(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
			data->kbytes_total += record->data.doc_size;
			data->memory_footprint += record->data.memory_footprint;
			PINBA_UPDATE_HISTOGRAM_ADD(report, data->histogram_data, record->data.req_time);
		}
#endif
	}
	pthread_rwlock_unlock(&report->lock);
	report->time_interval = pinba_get_time_interval();
	return report;
}
/* }}} */

