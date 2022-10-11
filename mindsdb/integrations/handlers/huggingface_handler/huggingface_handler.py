import pandas as pd
import transformers

from mindsdb.integrations.libs.base import BaseMLEngine


class HuggingFaceHandler(BaseMLEngine):
    name = 'huggingface'

    def create(self, target, args=None, **kwargs):

        args['target'] = target

        model_name = args['model_name']
        hf_model_storage_path = self.engine_storage.folder_get(model_name)  # real

        if args['task'] == 'translation':
            args['task_proper'] = f"translation_{args['lang_input']}_to_{args['lang_output']}"
        else:
            args['task_proper'] = args['task']

        print(f"Checking file system for {model_name}...")

        ####
        # Check if pipeline has already been downloaded
        try:
            pipeline = transformers.pipeline(task=args['task_proper'], model=hf_model_storage_path,
                                             tokenizer=hf_model_storage_path)
            print(f'Model already downloaded!')
        ####
        # Otherwise download it
        except OSError:
            print(f"Downloading {model_name}...")
            pipeline = transformers.pipeline(task=args['task_proper'], model=model_name)

            pipeline.save_pretrained(hf_model_storage_path)

            print(f"Saved to {hf_model_storage_path}")
        ####

        if 'max_length' in args:
            pass
        elif 'max_position_embeddings' in pipeline.model.config.to_dict().keys():
            args['max_length'] = pipeline.model.config.max_position_embeddings
        elif 'max_length' in pipeline.model.config.to_dict().keys():
            args['max_length'] = pipeline.model.config.max_length
        else:
            print('No max_length found!')

        if 'labels' in args:
            labels_default = pipeline.model.config.id2label
            labels_map = {}
            for num in labels_default.keys():
                labels_map[labels_default[num]] = args['labels'][num]
            args['labels_map'] = labels_map
        else:
            args['labels_map'] = None

        ###### store and persist in model folder
        self.model_storage.json_set('args', args)

        ###### persist changes to handler folder
        self.engine_storage.folder_sync(model_name)

    def predict(self, df):

        def tidy_output_classification(args, result):
            final = {}
            explain = {}
            if type(result) == dict:
                result = [result]
            final[args['target']] = args['labels_map'][result[0]['label']]
            for elem in result:
                if args['labels_map']:
                    explain[args['labels_map'][elem['label']]] = elem['score']
                else:
                    explain[elem['label']] = elem['score']
            final[f"{args['target']}_explain"] = explain

            return final

        def tidy_output_zero_shot(args, result):
            final = {}
            final[args['target']] = result['labels'][0]

            explain = dict(zip(result['labels'], result['scores']))
            final[f"{args['target']}_explain"] = explain

            return final

        def tidy_output_translation(args, result):
            final = {}
            final[args['target']] = result['translation_text']

            return final

        def tidy_output_summarization(args, result):
            final = {}
            final[args['target']] = result['summary_text']

            return final

        ###### get stuff from model folder
        args = self.model_storage.json_get('args')

        hf_model_storage_path = self.engine_storage.folder_get(args['model_name'])

        pipeline = transformers.pipeline(task=args['task_proper'], model=hf_model_storage_path,
                                         tokenizer=hf_model_storage_path)

        input_list = df[args['input_column']]
        input_list_str = [str(x) for x in input_list]

        task = args['task']
        if task == 'text-classification':
            output_list_messy = pipeline(input_list_str, truncation=True, num_workers=1, max_length=args['max_length'])
            output_list_tidy = [tidy_output_classification(args, x) for x in output_list_messy]

        elif task == 'zero-shot-classification':
            output_list_messy = pipeline(input_list_str, num_workers=1, candidate_labels=args['candidate_labels'],
                                         truncation=True, top_k=1000, max_length=args['max_length'])
            output_list_tidy = [tidy_output_zero_shot(args, x) for x in output_list_messy]

        elif task == 'translation':
            output_list_messy = pipeline(input_list_str, num_workers=1, max_length=args['max_length'])
            output_list_tidy = [tidy_output_translation(args, x) for x in output_list_messy]

        elif task == 'summarization':
            output_list_messy = pipeline(input_list_str, num_workers=1,
                                         min_length=args['min_output_length'],
                                         max_length=args['max_output_length'])
            output_list_tidy = [tidy_output_summarization(args, x) for x in output_list_messy]
        else:
            raise RuntimeError(f'Unknown task: {task}')
        pred_df = pd.DataFrame(output_list_tidy)

        return pred_df
