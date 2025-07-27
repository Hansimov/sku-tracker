is_scrape=false
is_extract=false
is_postprocess=false
is_weekly=false

for arg in "$@"; do
    if [ "$arg" = "-s" ]; then
        is_scrape=true
    elif [ "$arg" = "-e" ]; then
        is_extract=true
    elif [ "$arg" = "-x" ]; then
        is_postprocess=true
    elif [ "$arg" = "-w" ]; then
        is_weekly=true
    fi
done

if [ "$is_scrape" = true ]; then
    # zepto must run first, as it uses proxy, and would close browser when finished
    python -m web.zepto.batcher -s
    python -m web.blinkit.batcher -s
    # swiggy must run last, as it uses data of blinkit and zepto
    python -m web.swiggy.batcher -s
fi

if [ "$is_extract" = true ]; then
    python -m web.zepto.batcher -e
    python -m web.blinkit.batcher -e
    python -m web.swiggy.batcher -e
fi

if [ "$is_postprocess" = true ]; then
    python -m file.excel_merger -m
    python -m file.email
fi

if [ "$is_weekly" = true ]; then
    python -m file.excel_merger -p
    python -m file.email -c -t weekly
fi

# Usage:
# ./cli/run.sh            # do nothing
# ./cli/run.sh -s         # scrape only
# ./cli/run.sh -e         # extract only
# ./cli/run.sh -x         # post-process only
# ./cli/run.sh -e -x      # extract and post-process
# ./cli/run.sh -s -e -x   # scrape, extract and post-process
# ./cli/run.sh -w         # weekly summary