import csv

class CSVParser():
    ''' class to create dictionaries suited to creating objects in myTardis from a CSV file'''
    def __init__(self,
                 csv_file,
                 schema_dict,
                 expt_headers,
                 expt_title_header,
                 dataset_headers,
                 dataset_description_header,
                 datafile_headers,
                 datafile_name_header,
                 rel_path_header,
                 top_folder,
                 expt_id_header = None,
                 dataset_id_header = None):
        '''Initalise Parser

        ToDO: document this'''
        expt_title_ind = None
        if expt_id_header is None:
            expt_id_header = expt_title_header # Likely to cause issues
        if dataset_id_header is None:
            dataset_id_header = dataset_description_header # Likely to cause issues
        expt_id_ind = None
        expt_meta_ind = []
        dataset_description_ind = None
        dataset_id_ind = None
        dataset_meta_ind = []
        datafile_name_ind = None
        datafile_meta_ind = []
        rel_path_ind = None
        self.schema_dict = schema_dict
        self.experiments = []
        self.datasets = []
        self.datafiles = []
        self.top_folder = top_folder
        with open(csv_file, newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            header = next(reader)
            for column in header:
                if expt_id_header and column.lower() == expt_id_header:
                    expt_id_ind = header.index(column)
                if dataset_id_header and column.lower() == dataset_id_header:
                    dataset_id_ind = header.index(column)
                if column.lower() == datafile_name_header:
                    datafile_name_ind = header.index(column)
                if column.lower() == rel_path_header:
                    rel_path_ind = header.index(column)
                if column.lower() == expt_title_header:
                    expt_title_ind = header.index(column)
                if column.lower() == dataset_description_header:
                    dataset_description_ind = header.index(column)
                if column.lower() in expt_headers:
                    if column.lower() == expt_title_header:
                        expt_title_ind = header.index(column)
                    elif expt_id_header and column.lower() == expt_id_header:
                        pass
                    else:
                        expt_meta_ind.append(header.index(column))
                if column.lower() in dataset_headers:
                    if column.lower() == dataset_description_header:
                        dataset_description_ind = header.index(column)
                    elif dataset_id_header and column.lower() == dataset_id_header:
                        pass
                    dataset_meta_ind.append(header.index(column))
                if column.lower() in datafile_headers:
                    if column.lower() == datafile_name_header:
                        pass
                    elif column.lower() == rel_path_header:
                        pass
                    else:
                        datafile_meta_ind.append(header.index(column))
            for row in reader:
                expt_meta = {}
                dataset_meta = {}
                datafile_meta = {}
                expt_id = row[expt_id_ind]
                dataset_internal_id = expt_id
                if dataset_id_header == dataset_description_header: # Create a composite header
                    dataset_id = f'{row[expt_id_ind]}-{row[dataset_id_ind]}'
                else:
                    dataset_id = row[dataset_id_ind]
                datafile_dataset_id = dataset_id
                expt_title= row[expt_title_ind]
                for ind in expt_meta_ind:
                    expt_meta['Experiment_'+header[ind].lower().replace(' ', '_')] = row[ind]
                dataset_description = row[dataset_description_ind]
                for ind in dataset_meta_ind:
                    dataset_meta['Dataset_'+header[ind].lower().replace(' ', '_')] = row[ind]
                datafile_name = row[datafile_name_ind]
                for ind in datafile_meta_ind:
                    datafile_meta['Datafile_'+header[ind].lower().replace(' ', '_')] = row[ind]
                rel_path = row[rel_path_ind]
                self.experiments.append([expt_id,expt_title,expt_meta])
                self.datasets.append([dataset_internal_id, dataset_id, dataset_description, dataset_meta])
                self.datafiles.append([datafile_dataset_id, datafile_name, datafile_meta, rel_path])
        past_expt = 0
        indices = []
        for ind in range(len(self.experiments)):
            expt = self.experiments[ind]
            if past_expt == expt:
                indices.append(ind)
            indices = sorted(indices,reverse=True)
            past_expt = expt
        for ind in indices:
            self.experiments.pop(ind)
        past_dataset = 0
        indices = []
        for ind in range(len(self.datasets)):
            dataset = self.datasets[ind]
            if past_dataset == dataset:
                indices.append(ind)
            indices = sorted(indices,reverse=True)
            past_dataset = dataset
        for ind in indices:
            self.datasets.pop(ind)
        past_dataset = None
        indices = []
        past_indices = []
        for ind in range(len(self.datasets)):
            dataset = self.datasets[ind]
            if past_dataset:
                if dataset[0] == past_dataset[0] and dataset[1] == past_dataset[1]:
                    indices.append(ind)
                    past_indices.append(ind-1)
                else:
                    past_dataset = dataset
            else:
                past_dataset = dataset
        indices = sorted(indices, reverse=True)
        for ind in range(len(indices)):
            old = self.datasets.pop(indices[ind])[3]
            old_list = self.datasets.pop(past_indices[ind])
            new = old_list[3]
            for key in old.keys():
                if key in new.keys():
                    old_value = old[key]
                    new_value = new[key]
                    value = new_value
                    # Assume values are strings - need to think about how to handle other data types
                    if old_value != new_value:
                        value = f'{new_value}, {old_value}'
                    new[key] = value
                else:
                    value = old[key]
                    new[key] = value
            new_list = []
            new_list.append(old_list[0])
            new_list.append(old_list[1])
            new_list.append(old_list[2])
            new_list.append(new)
            self.datasets.append(new_list)
                    

    def create_expt_dicts(self,
                          users = None):
        experiments = []
        for current_expt in self.experiments:
            expt_dict = {'title': current_expt[1],
                         'internal_id': current_expt[0],
                         'schema_namespace': self.schema_dict['experiment']}
            expt_dict.update(current_expt[2])
            if users:
                expt_dict.update(users)
            experiments.append(expt_dict)
        return experiments

    def create_dataset_dicts(self):
        datasets = []
        for current_dataset in self.datasets:
            dataset_dict = {'internal_id': current_dataset[0],
                            'dataset_id': current_dataset[1],
                            'description': current_dataset[2],
                            'schema_namespace': self.schema_dict['dataset']}
            dataset_dict.update(current_dataset[3])
            datasets.append(dataset_dict)
        return datasets

    def create_datafile_dicts(self,
                              in_store=True,
                              bucket='mytardis'):
        datafiles = []
        for current_datafile in self.datafiles:
            datafile_dict = {'file_name': current_datafile[1],
                             'rel_path': current_datafile[3],
                             'dataset_id': current_datafile[0],
                             'in_store': in_store,
                             'schema_namespace': self.schema_dict['datafile']}
            if in_store:
                datafile_dict['s3_path'] = current_datafile[3]
                datafile_dict['bucket'] = bucket
            datafile_dict.update(current_datafile[2])
            pop_keys = []
            for key in datafile_dict.keys():
                if datafile_dict[key] == '':
                    pop_keys.append(key)
            for key in pop_keys:
                datafile_dict.pop(key)
            datafiles.append(datafile_dict)
        return datafiles
