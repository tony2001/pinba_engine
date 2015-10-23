
static inline pinba_report *PINBA_REGENERATE_REPORT_FUNC_D()/* pinba_regenerate_report9(PINBA_SHARE *share) */ /* {{{ */
{
	pinba_report *report;

	report = (pinba_report *)pinba_map_get(D->base_reports, share->index);
	if (!report) {

		report = (pinba_report *)calloc(1, sizeof(pinba_report));
		if (!report) {
			return NULL;
		}

		pinba_parse_conditions(share, (pinba_std_report *)report);

		report->std.index = strdup(share->index);
		report->std.type = PINBA_REPORT_ID()/*PINBA_TABLE_REPORT9*/;
		report->std.time_interval = 1;
		report->std.add_func = PINBA_UPDATE_REPORT_ADD_FUNC()/*pinba_update_report9_add*/;
		report->std.delete_func = PINBA_UPDATE_REPORT_DELETE_FUNC()/*pinba_update_report9_delete*/;
		pthread_rwlock_init(&report->std.lock, 0);

		D->base_reports = pinba_map_add(D->base_reports, share->index, report);

		if (pinba_array_add(&D->base_reports_arr, report) < 0) {
			pinba_map_delete(D->base_reports, share->index);
			pthread_rwlock_destroy(&report->std.lock);
			free(report);
			return NULL;
		}

		if (!pinba_update_report_tables((pinba_std_report *)report, share->index)) {
			pinba_array_delete(&D->base_reports_arr, report);
			pinba_map_delete(D->base_reports, share->index);
			pthread_rwlock_destroy(&report->std.lock);
			free(report);
			return NULL;
		}

	} else {
		return report;
	}

	return report;
}
/* }}} */

