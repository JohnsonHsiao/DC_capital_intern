import pyautogui
import time

# Give yourself some time to focus on the input area (e.g., a text editor) where you want the text to be typed.

time.sleep(5)

while True:
    pyautogui.write('/dice')
    
    time.sleep(1)

    pyautogui.press('enter')
    
    time.sleep(1)
    
    pyautogui.write('50')
    
    time.sleep(1)
    
    pyautogui.press('enter')

    time.sleep(61)
