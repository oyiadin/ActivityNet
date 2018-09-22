#source activate activitynet
python3 download.py just_for_testing --train test.csv -n 2
echo '用同一个环境(source activate activitynet)即可'
echo '这个脚本只会下载文件，没有进行切割(我有点没看懂这些csv的规则…)'
echo '没问题的话运行下面的命令就可以开始下载了：'
echo 'python3 download.py dataset_ava --train data/ava_train_v2.1.csv --val ava_val_v2.1.csv --test ava_test_v2.1.txt'
