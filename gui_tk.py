import tkinter

top = tkinter.Tk()
label = tkinter.Label(top, text='Hello, world!')
label.pack()
quit = tkinter.Button(top, text='QUIT!', command=top.quit, bg='red', fg='white')
quit.pack(fill=tkinter.X, expand=1)
tkinter.mainloop()