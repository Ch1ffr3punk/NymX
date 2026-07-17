```
$ nymx
NymX - (c) 2026 Ch1ffr3punk
Anonymous data exchange leveraging SURBs via the Nym Mixnet

Usage:
  nymx -r [-p <path>]               - Receive mode: Listen for incoming files
                                      -p: Path to save files (default: ./received)
  nymx -s <target> <file>           - Send mode: Send a file anonymously
  nymx -s --part <target> <prefix>  - Send parts mode: Send multiple parts sequentially
  nymx -g                           - Get mode: Download files from SSH server via Tor

Examples:
  nymx -r
  nymx -r -p /var/spool/received
  nymx -s AliceNymAddress document.pdf
  nymx -s alice document.pdf (using alias from nymx.json)
  nymx -s --part alice movie.mp4.part
  nymx -g

Example nymx.json (for -s and -g):
  {
    "aliases": {
      "alice": "AliceNymAddress",
      "bob": "BobNymAddress",
      "carol": "CarolNymAddress"
    },
    "ssh": {
      "host": "abcdef1234567890.onion",
      "port": 22,
      "username": "Ch1ffr3punk",
      "socks5_proxy": "127.0.0.1:9050"
    }
  }

Note: The receiver (-r) does NOT need nymx.json. Use -p to specify the save path.
Note: For --part mode, ensure sam's ripemd-160.txt exists in the current directory.

Receiving files:

$ nymx -r

Sending a file:

$ nymx -a alice file
```

There is a helper utility called ['sam'](https://github.com/Ch1ffr3punk/sam) available to split large files.  
Tests have shown that splitting files in parts of 5 MiB each are working best with the Nym Mixnet.  

If you are a privacy enthusiast and use NymX on a regular basis  
consider a small donation in crypto currencies or buy me a coffee.    
```
BTC: bc1qm0e7r94ht60tu7zuewf0ftl3td0xc700rvcagn
Nym: n1f0r6zzu5hgh4rprk2v2gqcyr0f5fr84zv69d3x          
```
<a href="https://www.buymeacoffee.com/Ch1ffr3punk" target="_blank"><img src="https://cdn.buymeacoffee.com/buttons/default-yellow.png" alt="Buy Me A Coffee" height="41" width="174"></a>

NymX is dedicated to Alice and Bob.