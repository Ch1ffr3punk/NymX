```
$ nymx -h
A data exchange tool for the Nym Mixnet

Usage: nymx [OPTIONS] [FILE]

Arguments:
  [FILE]

Options:
  -t, --receiver <RECEIVER_ADDR>
  -r, --receive
  -o, --out <OUTPUT_DIR>            [default: ./received]
  -c, --chunk-size <CHUNK_SIZE_KB>  [default: 64]
  -R, --rate <CHUNKS_PER_SECOND>    [default: 1]
  -a, --alias <ALIAS_NAME>
  -w, --whitelist <WHITELIST>
  -q, --quota <QUOTA_MIB>
  -h, --help                        Print help
  -V, --version                     Print Version

Receiving files:

$ nymx -r -q 10 -w whitelist.json

Sending a file:

$ nymx -a alice file
```

There are two helper utilities for NymX available. ['sam'](https://github.com/Ch1ffr3punk/sam) to split    
large files and ['get'](https://github.com/Ch1ffr3punk/get) if you run NymX on a VPS, to fetch your files.   

If you are a privacy enthusiast and use NymX on a regular basis  
consider a small donation in crypto currencies or buy me a coffee.    
```
BTC: bc1qm0e7r94ht60tu7zuewf0ftl3td0xc700rvcagn
Nym: n1f0r6zzu5hgh4rprk2v2gqcyr0f5fr84zv69d3x          
```
<a href="https://www.buymeacoffee.com/Ch1ffr3punk" target="_blank"><img src="https://cdn.buymeacoffee.com/buttons/default-yellow.png" alt="Buy Me A Coffee" height="41" width="174"></a>

NymX is dedicated to Alice and Bob.