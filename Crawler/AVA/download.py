import argparse
import json
import os
import shutil
import subprocess
import uuid

from joblib import delayed, Parallel


def download_whole_file(id, tmp_dir='tmp', num_attempts=3,
                        url_base='https://www.youtube.com/watch?v='):
    # Construct command line for getting the direct video link.
    tmp_filename = os.path.join(tmp_dir, '{}.mp4'.format(uuid.uuid4()))
    command = ['youtube-dl',
               '"%s%s"' % (url_base, id),
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
            print('[{}] RETRYING {}/{}'.format(id, attempts, num_attempts-1))
        else:
            break

    print('[{}] downloaded'.format(id))
    return tmp_filename


def download_clip_wrapper(id, subset, output_dir, tmp_dir):
    """Wrapper for parallel processing purposes.
       Returns ( video_id, status: bool, message )"""
    assert isinstance(id, str) and len(id) == 11
    output_filename = os.path.join(output_dir, id+'.mp4')
    if os.path.exists(output_filename):
        print('[{}] file already existed, ignored.'.format(id))
        return id, True, 'ignored'

    try:
        tmp_filename = download_whole_file(id, tmp_dir)
    except subprocess.CalledProcessError as err:
        return id, False, '[download] ' + err.output.decode()

    if not os.path.exists(tmp_filename):
        message = '[download] cannot find the downloaded file ' \
                  'for unknown reason'
        print(message)
        return id, False, message

    os.rename(tmp_filename, output_filename)

    try:
        os.remove(tmp_filename or '')
    except FileNotFoundError:
        pass

    return id, True, 'OK'


def main(train, output_dir, num_jobs=24, verbose=False, tmp_dir='tmp',
         val=None, test=None):
    # Reading and parsing ActivityNet.
    os.makedirs(tmp_dir, exist_ok=True)
    status_lst = []

    for subset, filename in (
            ('training', train), ('validation', val), ('test', test)):
        print('==============================================')
        print('[prepare] processing {} subset...'.format(subset))
        if not filename:
            print('[prepare] no {} csv/txt file assigned, ignored'.format(subset))
            continue
        if verbose:
            verbose = 10

        path = os.path.join(output_dir, subset)
        os.makedirs(path, exist_ok=True)

        with open(filename) as f:
            unique_ids = set([line.split(',')[0].strip() for line in f])
            status_lst = Parallel(
                n_jobs=num_jobs,
                # backend='multiprocessing',
                verbose=verbose,
            )(delayed(download_clip_wrapper)(
                id, subset=subset, output_dir=path, tmp_dir=tmp_dir)
              for id in unique_ids if id)


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
    p.add_argument('--train', type=str, required=False, help='the training set')
    p.add_argument('--val', type=str, required=False, help='the validation set')
    p.add_argument('--test', type=str, required=False, help='the test set')
    p.add_argument('output_dir', type=str,
                   help='Output directory where videos will be saved.')
    p.add_argument('-n', '--num-jobs', type=int, default=10)
    p.add_argument('-t', '--tmp-dir', type=str, default='temp')
    p.add_argument('-v', '--verbose', type=bool, default=False)
    main(**vars(p.parse_args()))
