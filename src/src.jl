
rgpcentury = "([1-9][0-9]?)\\. (Jahrh|Jh)"
rgx1qcentury = Regex("(1\\.|erstes) Viertel (des )?" * rgpcentury, "i")
rgx2qcentury = Regex("(2\\.|zweites) Viertel (des )?" * rgpcentury, "i")
rgx3qcentury = Regex("(3\\.|drittes) Viertel (des )?" * rgpcentury, "i")
rgx4qcentury = Regex("(4\\.|viertes) Viertel (des )?" * rgpcentury, "i")


rgx1hcentury = Regex("(1\\.|erste) Hälfte (des )?" * rgpcentury, "i")
rgx2hcentury = Regex("(2\\.|zweite) Hälfte (des )?" * rgpcentury, "i")

rgxearlycentury = Regex("fühes " * rgpcentury, "i")
rgxlatecentury = Regex("spätes " * rgpcentury, "i")

rgxbegincentury = Regex("Anfang (des )?" * rgpcentury, "i")
rgxendcentury = Regex("Ende (des )?" * rgpcentury, "i")

rgxcentury = r"([1-9][0-9])?\. (Jahrh|Jhd)"
rgxyear = r"^ *[1-9][0-9][0-9]+"
