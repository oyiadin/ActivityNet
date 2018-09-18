import argparse
import json
import os
import shutil
import subprocess
import uuid

from joblib import delayed, Parallel

taxonomy = []
label2id = {}


def construct_path_to_video(id, subset, annotation=None):
    if subset == 'testing':
        return os.path.join('testing', '{}.mp4'.format(str(id)))

    assert annotation
    label_id = label2id[annotation['label']]
    begin, end = annotation['segment']

    basename = '%s_%s_%d_%.2f_%.2f.mp4' % (subset, id, label_id, begin, end)
    path = taxonomy[label_id].get_dir()
    return os.path.join(path, basename)


def download_whole_file(id, url, tmp_dir='tmp',
                        num_attempts=3):
    # Construct command line for getting the direct video link.
    tmp_filename = os.path.join(tmp_dir, '{}.mp4'.format(uuid.uuid4()))
    command = ['youtube-dl',
               '"%s"' % url,
               '--quiet',
               '--no-warnings',
               '-f', 'mp4',
               '-o', '"%s"' % tmp_filename]
    command = ' '.join(command)
    attempts = 0
    print('[{}] downloading'.format(id))
    while True:
        try:
            output = subprocess.check_output(command, shell=True,
                                             stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as err:
            print('[{}] an error occurred when downloading: {}'.format(
                id, err.output.decode().strip('\n')))
            attempts += 1
            if attempts == num_attempts:
                print('[{}] cannot download, gave up.'.format(id))
                raise err
            print('[{}] RETRYING {}/{}'.format(id, attempts+1, num_attempts))
        else:
            break

    print('[{}] downloaded'.format(id))
    return tmp_filename


def gen_trimmed_video(id, begin, end, file_from, file_to):
    # Construct command to trim the videos (ffmpeg required).
    command = ['ffmpeg',
               '-i', '"%s"' % os.path.abspath(file_from),
               '-ss', str(begin),
               '-t', str(end - begin),
               '-c:v', 'libx264', '-c:a', 'copy',
               '-threads', '1',
               '-loglevel', 'panic',
               '-y',
               '"%s"' % os.path.abspath(file_to)]
    command = ' '.join(command)
    print('[{}] trimming'.format(id))
    try:
        output = subprocess.check_output(command, shell=True,
                                         stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as err:
        print('[{}] an error occurred when do trimming: {}'.format(
            id, err.output.decode().strip('\n')))
        raise err

    print('[{}] trimmed, ffmpeg output={}'.format(id, output.decode()))


def download_clip_wrapper(id, infos, output_dir, tmp_dir):
    """Wrapper for parallel processing purposes.
       Returns ( video_id, status: bool, message )"""
    tmp_filename = ''
    if infos['subset'] == 'testing':
        output_filename = os.path.join(
            output_dir, construct_path_to_video(id, 'testing'))
        if os.path.exists(output_filename):
            print('[{}] file already existed, ignored.'.format(id))
            return id, True, 'ignored'

        try:
            tmp_filename = download_whole_file(id, infos['url'], tmp_dir)
        except Exception as err:
            return id, False, '[download] ' + err.output.decode()

        if not os.path.exists(tmp_filename):
            return id, False, '[download] cannot find the downloaded file ' \
                              'for unknown reason'

        os.rename(tmp_filename, output_filename)

    else:
        tmp_filename = None

        for n, annotation in enumerate(infos['annotations']):
            output_filename = os.path.join(
                output_dir, construct_path_to_video(
                    id, infos['subset'], annotation))
            if os.path.exists(output_filename):
                print('[{}] file already existed, ignored.'.format(id))
                continue
            begin, end = annotation['segment']

            try:
                if not tmp_filename:
                    tmp_filename = download_whole_file(id, infos['url'], tmp_dir)
            except Exception as err:
                return id, False, '[download] ' + err.output.decode()

            if not os.path.exists(tmp_filename):
                return id, False, '[download] cannot find the downloaded file' \
                                  ' for unknown reason'

            try:
                gen_trimmed_video(id, begin, end, tmp_filename, output_filename)
            except subprocess.CalledProcessError as err:
                return id, False, \
                       '[trimming {}_annotation_{}] {}'.format(
                           id, n, err.output.decode())
            if not os.path.exists(output_filename):
                return id, False, '[trimming {}_annotation_{}] cannot find ' \
                                  'the trimmed file ' \
                                  'for unknown reason'.format(id, n)
    try:
        os.remove(tmp_filename or '')
    except FileNotFoundError:
        pass

    return id, True, 'OK'


class Node(object):
    def __init__(self, one_node: dict):
        """every instance represents a node in the 'taxonomy' field

        `one_node` is something like:
        {
          "parentName": "Health-related self care",
          "nodeName": "Applying sunscreen",
          "nodeId": 389,
          "parentId": 269
        }"""
        self.parent_id = -1 if one_node['parentId'] is None \
            else int(one_node['parentId'])
        self.id = int(one_node['nodeId'])
        self.name = one_node['nodeName']

    def get_dir(self):
        if self.parent_id == -1:
            return ''
        else:
            return os.path.join(
                taxonomy[self.parent_id].get_dir(),
                '{}_{}'.format(self.id, self.name.replace(os.sep, 'or')))


def prepare(input_json, output_dir):
    global taxonomy, label2id
    taxonomy = {}
    the_json = json.load(open(input_json))
    for i in the_json['taxonomy']:
        taxonomy[i['nodeId']] = Node(i)
        label2id[i['nodeName']] = i['nodeId']
    for i in taxonomy.values():
        os.makedirs(os.path.join(output_dir, i.get_dir()), exist_ok=True)
    os.makedirs(os.path.join(output_dir, 'testing'), exist_ok=True)
    print('[prepare] taxonomy folders created')
    return the_json['database']


def main(input_json, output_dir, num_jobs=24, verbose=False,
         tmp_dir='tmp'):
    # Reading and parsing ActivityNet.
    os.makedirs(tmp_dir, exist_ok=True)
    database = prepare(input_json, output_dir)
    if verbose:
        verbose = 10

    status_lst = Parallel(
        n_jobs=num_jobs,
        # backend='multiprocessing',
        verbose=verbose,
    )(delayed(download_clip_wrapper)(
        id, infos, output_dir, tmp_dir) for id, infos in database.items())

    # Clean tmp dir.
    try:
        shutil.rmtree(tmp_dir)
    except FileNotFoundError:
        pass

    # Save download report.
    with open('download_report.json', 'w') as fobj:
        fobj.write(json.dumps(status_lst))


if __name__ == '__main__':
    description = 'Helper script for downloading and trimming Activity Net' \
                  'videos.'
    p = argparse.ArgumentParser(description=description)
    p.add_argument('input_json', type=str,
                   help='JSON file containing the required information')
    p.add_argument('output_dir', type=str,
                   help='Output directory where videos will be saved.')
    p.add_argument('-n', '--num-jobs', type=int, default=24)
    p.add_argument('-t', '--tmp-dir', type=str, default='tmp')
    p.add_argument('-v', '--verbose', type=bool, default=False)
    main(**vars(p.parse_args()))
