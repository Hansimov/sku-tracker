## Install dependencies

```sh
sudo apt-get install xvfb xserver-xephyr tigervnc-standalone-server x11-utils gnumeric
pip install pyvirtualdisplay pillow EasyProcess pyautogui mss
```

> [!NOTE]
> DrissionPage 文档
> * https://drissionpage.cn/browser_control/intro/
> 
> ponty/PyVirtualDisplay: Python wrapper for Xvfb, Xephyr and Xvnc
> * https://github.com/ponty/PyVirtualDisplay
> 
> Welcome to PyAutoGUI’s documentation! — PyAutoGUI documentation
> * https://pyautogui.readthedocs.io/en/latest/


## Common issues

```sh
Missing X server or $DISPLAY
The platform failed to initialize.  Exiting.
```

This issue mostly happens when ssh is terminated unexpectedly.

In my case, `$DISPLAY` is set to `localhost:11.0` by default.

And the solution is to resolve `$DISPLAY` environment correctly.


## Solution 1

```sh
# xdpyinfo -display :10.0
export DISPLAY=localhost:10.0
```

And if rebooted or re-connected, you might need to reset:

```sh
# xdpyinfo -display :11.0
export DISPLAY=localhost:11.0
```

> [!NOTE]
> [Bug]: Missing X server or $DISPLAY · Issue #8148 · puppeteer/puppeteer
>   * https://github.com/puppeteer/puppeteer/issues/8148
>   * https://github.com/puppeteer/puppeteer/issues/8148#issuecomment-3095573227

## Solution 2

```sh
# sudo apt-get install -y xvfb
# sudo apt-get -y install xorg xvfb gtk2-engines-pixbuf dbus-x11 xfonts-base xfonts-100dpi xfonts-75dpi xfonts-cyrillic xfonts-scalable
```

```sh
Xvfb -ac :99 -screen 0 1280x1024x16 &
export DISPLAY=:99
```

This will start a virtual display on port `:99`, but it is invisible to user connected via ssh.

> [!NOTE] 
> ssh - Unable to open X display when trying to run google-chrome on Centos (Rhel 7.5) - Stack Overflow
>   * https://stackoverflow.com/questions/60304251/unable-to-open-x-display-when-trying-to-run-google-chrome-on-centos-rhel-7-5


## Solution 3

```txt
D-Bus connection was disconnected. Aborting.
```

```sh
echo $DBUS_SESSION_BUS_ADDRESS
# unix:path=/run/user/1000/bus
```

```sh
export DBUS_SESSION_BUS_ADDRESS=none
```