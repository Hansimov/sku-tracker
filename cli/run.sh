is_scrape=false
is_extract=false
is_postprocess=false
is_weekly=false
date_arg=""

i=0
for arg in "$@"; do
    i=$((i + 1))
    if [ "$arg" = "-s" ]; then
        is_scrape=true
    elif [ "$arg" = "-e" ]; then
        is_extract=true
    elif [ "$arg" = "-x" ]; then
        is_postprocess=true
    elif [ "$arg" = "-w" ]; then
        is_weekly=true
    elif [ "$arg" = "-d" ]; then
        next_i=$((i + 1))
        eval "date_value=\${$next_i}"
        if [ -n "$date_value" ]; then
            date_arg="-d $date_value"
        fi
    fi
done

# -s
if [ "$is_scrape" = true ]; then
    # zepto must run first, as it uses proxy, and would close browser when finished
    python -m web.zepto.batcher -s $date_arg
    python -m web.blinkit.batcher -s $date_arg
    python -m web.dmart.batcher -s $date_arg
    # swiggy must run last, as it uses data of blinkit and zepto
    python -m web.swiggy.batcher -s $date_arg
fi

# -e
if [ "$is_extract" = true ]; then
    python -m web.zepto.batcher -e $date_arg
    python -m web.blinkit.batcher -e $date_arg
    python -m web.swiggy.batcher -e $date_arg
    python -m web.dmart.batcher -e $date_arg
fi

# -x
if [ "$is_postprocess" = true ]; then
    python -m file.excel_merger -m -k $date_arg
    python -m file.email $date_arg
fi

# -w
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

# ./cli/run.sh -e -d "2025-08-19"  # extract for date