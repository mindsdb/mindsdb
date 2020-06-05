import random
from mindsdb.external_libs.stats import calculate_sample_size


def sample_data(df, sample_margin_of_error, sample_confidence_level, log):
    population_size = len(df)

    sample_size = int(calculate_sample_size(population_size, sample_margin_of_error, sample_confidence_level)) if population_size > 50 else population_size
    sample_size_pct = sample_size*100/population_size

    # get the indexes of randomly selected rows given the population size
    input_data_sample_indexes = random.sample(range(population_size), sample_size)

    log.info(f'Analyzing a sample of {sample_size} from a total population of {population_size}, this is equivalent to {sample_size_pct}% of your data.')

    return df.iloc[input_data_sample_indexes]
