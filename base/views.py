from django.http import HttpResponse, JsonResponse
from django.shortcuts import render


def home(request):
    return HttpResponse('<h1>No hay informacioón disponible</h1>')
