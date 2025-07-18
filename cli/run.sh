# zepto must be run first, as it uses proxy, and would close browser when finished
python -m web.zepto.batcher -s -e
python -m web.blinkit.batcher -s -e
# swiggy must be run last, as it uses data of blinkit and zepto
python -m web.swiggy.batcher -s -e
python -m file.excel_merger

# ./cli/run.sh