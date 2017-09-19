from observations.observations import get_and_insert_current_predictions_by_routes

new_records_count, updated_records_count = get_and_insert_current_predictions_by_routes()
print('{} new records inserted, {} rows updated'.format(new_records_count, updated_records_count))
