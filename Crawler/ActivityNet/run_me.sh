echo '跟 Kinetics 的用法基本一样：python3 download.py input_json output_dir'
python3 download.py test.json just_for_test
echo '预期表现:'
echo '1. “-----------”这个文件会重复尝试三次并下载失败'
echo '2. just_for_test 下各个目录都有且只有一个可以播放的 mp4 文件'
echo '3. download_report.json 除了 ----------- 之外都是 OK'
echo '没问题的话直接运行 python3 download.py data/activity_net.v1-3.min.json dataset 就可以开始下载了'
