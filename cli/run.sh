is_scrape=false
is_extract=false
is_postprocess=false

for arg in "$@"; do
    if [ "$arg" = "-s" ]; then
        is_scrape=true
    elif [ "$arg" = "-e" ]; then
        is_extract=true
    elif [ "$arg" = "-x" ]; then
        is_postprocess=true
    fi
done

if [ "$is_scrape" = true ]; then
    # zepto must run first, as it uses proxy, and would close browser when finished
    python -m web.zepto.scraper -s -c
    python -m web.blinkit.scraper -s
    # swiggy must run last, as it uses data of blinkit and zepto
    python -m web.swiggy.scraper -s -c
fi

if [ "$is_extract" = true ]; then
    python -m web.zepto.batcher -e
    python -m web.blinkit.batcher -e
    python -m web.swiggy.batcher -e
fi

if [ "$is_postprocess" = true ]; then
    python -m file.excel_merger
    python -m file.email
fi

# Usage:
# ./cli/run.sh            # do nothing
# ./cli/run.sh -s         # scrape only
# ./cli/run.sh -e         # extract only
# ./cli/run.sh -x         # post-process only
# ./cli/run.sh -e -x      # extract and post-process
# ./cli/run.sh -s -e -x   # scrape, extract and post-process